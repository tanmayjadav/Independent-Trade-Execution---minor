from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Tuple

from reporting.discord import DiscordAlert
from market.market_clock import MarketClock
from utils.logger import get_logger


SENSITIVE_KEY_SUBSTRINGS = (
    "webhook",
    "password",
    "api_key",
    "token",
    "totp",
    "secret",
    "mongo_uri",
    "client_code",
)


def _is_sensitive_key(key: str) -> bool:
    k = (key or "").lower()
    return any(s in k for s in SENSITIVE_KEY_SUBSTRINGS)


def sanitize_config(obj: Any) -> Any:
    """
    Recursively sanitize config structures for sending to Discord.
    Redacts values for keys that look sensitive (webhooks, passwords, tokens, etc.).
    """
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if _is_sensitive_key(str(k)):
                out[k] = "<REDACTED>"
            else:
                out[k] = sanitize_config(v)
        return out
    if isinstance(obj, list):
        return [sanitize_config(x) for x in obj]
    return obj


def _json_compact(obj: Any, max_chars: int = 1900) -> str:
    """
    Compact-ish JSON for Discord (keep under Discord 2000 char text limit).
    """
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception:
        s = str(obj)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."


def _mongo_healthcheck(mongo_uri: str) -> Tuple[bool, str]:
    """
    Returns (ok, message). Avoids leaking the uri itself.
    """
    if not mongo_uri:
        return False, "mongo_uri not configured"
    try:
        # Reuse existing client factory (short timeout configured there)
        from database.mongo_client import MongoDBClient

        client = MongoDBClient.get_client(mongo_uri)
        client.admin.command("ping")
        return True, "ok"
    except Exception as e:
        return False, f"failed: {type(e).__name__}"


def _rest_ltp_check(md_streamer, contract) -> Tuple[bool, str]:
    """
    Best-effort REST LTP check using the same logic as trade-controller REST fallback.
    """
    try:
        from execution.trade_controller import TradeController

        # Minimal dummy wiring; only _get_rest_ltp is used
        tc = TradeController(
            broker=None,
            option_selector=None,
            risk_manager=None,
            config={},
            trade_repo=None,
            md_streamer=md_streamer,
        )
        ltp = tc._get_rest_ltp(contract)
        if ltp and float(ltp) > 0:
            return True, f"{float(ltp):.2f}"
        return False, "0.0"
    except Exception as e:
        return False, f"failed: {type(e).__name__}"


@dataclass(frozen=True)
class PreMarketSchedule:
    send_at: datetime
    minutes_before_open: int


def compute_today_schedule(minutes_before_open: int) -> PreMarketSchedule:
    now = datetime.now()
    m_open = MarketClock.get_market_open()
    market_open_dt = now.replace(
        hour=m_open.hour, minute=m_open.minute, second=0, microsecond=0
    )
    send_at = market_open_dt - timedelta(minutes=int(minutes_before_open))
    return PreMarketSchedule(send_at=send_at, minutes_before_open=int(minutes_before_open))


def _sentinel_path(log_dir: str | Path) -> Path:
    day = datetime.now().strftime("%Y%m%d")
    return Path(log_dir) / f"premarket_sent_{day}.flag"


def already_sent_today(log_dir: str | Path = "logs") -> bool:
    try:
        return _sentinel_path(log_dir).exists()
    except Exception:
        return False


def mark_sent_today(log_dir: str | Path = "logs") -> None:
    try:
        p = _sentinel_path(log_dir)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(datetime.now().isoformat(), encoding="utf-8")
    except Exception:
        pass


def send_pre_market_notifications(
    *,
    config: dict,
    credentials_loaded: bool,
    is_paper: bool,
    md_broker,
    instrument_manager,
    underlying_contract,
    md_streamer,
    log_dir: str | Path = "logs",
) -> None:
    """
    Sends:
    - Daily checklist confirmation to deployment.discord_webhook_checks
    - Sanitized config snapshot to deployment.discord_webhook_configs
    """
    logger = get_logger("status")
    deployment = config.get("deployment", {}) or {}

    checks_webhook = deployment.get("discord_webhook_checks", "")
    configs_webhook = deployment.get("discord_webhook_configs", "")

    # If either is missing, still run (but only send where configured)
    discord = DiscordAlert()

    # Checklist checks
    checks: list[tuple[str, bool, str]] = []

    checks.append(("mode.paper_trading", bool(is_paper), "paper" if is_paper else "live"))
    checks.append(("credentials.loaded", bool(credentials_loaded), "ok" if credentials_loaded else "missing/empty"))

    # Market-data broker connectivity (paper mode usually)
    md_ok = bool(md_broker is not None)
    checks.append(("market_data.broker_ready", md_ok, "ok" if md_ok else "not_connected"))

    # Instruments loaded
    inst_count = 0
    try:
        inst_df = getattr(instrument_manager, "instruments", None)
        inst_count = int(len(inst_df)) if inst_df is not None else 0
    except Exception:
        inst_count = 0
    checks.append(("instruments.count", inst_count > 0, str(inst_count)))

    # Underlying resolved
    u_ok = bool(underlying_contract is not None) and hasattr(underlying_contract, "token")
    u_sym = getattr(underlying_contract, "symbol", "N/A")
    u_tok = getattr(underlying_contract, "token", "N/A")
    u_ex = getattr(underlying_contract, "exchange", "N/A")
    checks.append(("underlying.resolved", u_ok, f"{u_sym} | {u_ex} | token={u_tok}"))

    # REST LTP check (critical for “no ticks” cases)
    ltp_ok, ltp_msg = (False, "skipped")
    if u_ok and md_streamer is not None:
        ltp_ok, ltp_msg = _rest_ltp_check(md_streamer, underlying_contract)
    checks.append(("market_data.rest_ltp", ltp_ok, ltp_msg))

    # Mongo health
    mongo_uri = deployment.get("mongo_uri", "")
    mongo_ok, mongo_msg = _mongo_healthcheck(mongo_uri)
    checks.append(("db.mongo", mongo_ok, mongo_msg))

    # Decide GO/NO-GO: strict on key infra
    critical_keys = {
        "market_data.broker_ready",
        "instruments.count",
        "underlying.resolved",
        "market_data.rest_ltp",
    }
    failures = [name for (name, ok, _) in checks if (name in critical_keys and not ok)]
    go = len(failures) == 0

    # Build checklist message
    schedule_minutes = int(deployment.get("discord_alerts_time_before_market_open", 30))
    msg = {
        "title": f"Pre-market checklist ({schedule_minutes} min before open) — {'GO' if go else 'NO-GO'}",
        "date": datetime.now().strftime("%d %b %Y"),
        "color": "green" if go else "red",
        "time": datetime.now().strftime("%H:%M:%S"),
        "market_hours": MarketClock.get_market_hours_str(),
        "failures": ", ".join(failures) if failures else "none",
        "fields": [
            {"name": name, "value": ("✅ " if ok else "❌ ") + details, "inline": False}
            for (name, ok, details) in checks
        ],
    }

    if checks_webhook:
        discord.send_alert(webhook_url=checks_webhook, message=msg, use_embed=True)
        logger.info(f"Pre-market checklist sent | GO={go} | failures={failures}")

    # Config snapshot message (sanitized)
    sanitized = sanitize_config(config)
    cfg_text = _json_compact(sanitized)
    cfg_msg = {
        "title": "Config snapshot (sanitized)",
        "description": f"```json\n{cfg_text}\n```",
        "color": "blue",
        "date": datetime.now().strftime("%d %b %Y"),
        "time": datetime.now().strftime("%H:%M:%S"),
    }

    if configs_webhook:
        discord.send_alert(webhook_url=configs_webhook, message=cfg_msg, use_embed=True)
        logger.info("Pre-market config snapshot sent (sanitized)")

    mark_sent_today(log_dir=log_dir)


def send_trading_session_start_alert(
    *,
    config: dict,
    is_paper: bool,
    broker,
    md_broker,
    instrument_manager,
    underlying_contract,
) -> None:
    """
    One combined alert on trading session start.
    Sends to deployment.discord_webhook_alerts (fallback: deployment.discord_webhook).
    """
    logger = get_logger("startup")
    deployment = config.get("deployment", {}) or {}
    webhook = deployment.get("discord_webhook_alerts") or deployment.get("discord_webhook", "")
    if not webhook:
        return

    # Build details (safe, non-sensitive)
    trading_mode = "Paper Trading" if is_paper else "Live Trading"
    broker_name = type(broker).__name__ if broker is not None else "None"
    md_name = type(md_broker).__name__ if md_broker is not None else "None"

    inst_count = 0
    try:
        inst_df = getattr(instrument_manager, "instruments", None)
        inst_count = int(len(inst_df)) if inst_df is not None else 0
    except Exception:
        inst_count = 0

    u_sym = getattr(underlying_contract, "symbol", "N/A")
    u_tok = getattr(underlying_contract, "token", "N/A")
    u_ex = getattr(underlying_contract, "exchange", "N/A")

    risk = config.get("risk", {}) or {}
    execution = config.get("execution", {}) or {}

    msg = {
        "title": "Trading session started",
        "date": datetime.now().strftime("%d %b %Y"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "color": "green" if MarketClock.is_market_open() else "yellow",
        "market_open": "YES" if MarketClock.is_market_open() else "NO",
        "market_hours": MarketClock.get_market_hours_str(),
        "mode": trading_mode,
        "broker": broker_name,
        "market_data_broker": md_name,
        "underlying": f"{u_sym} | {u_ex} | token={u_tok}",
        "instruments_loaded": inst_count,
        "risk_mode": str(risk.get("mode", "")),
        "risk_value": str(risk.get("value", "")),
        "max_daily_loss": str(risk.get("max_daily_loss", "")),
        "max_daily_loss_percent": str(risk.get("max_daily_loss_percent", "")),
        "allow_multiple_positions": str(risk.get("allow_multiple_positions", "")),
        "order_type": str(execution.get("order_type", "")),
        "sl_percent": str(execution.get("sl_percent", "")),
        "tp_percent": str(execution.get("tp_percent", "")),
        "trailing_sl": str(execution.get("trailing_sl", "")),
        "breakeven_enabled": str(execution.get("breakeven_enabled", "")),
        "squareoff_time": str(execution.get("squareoff_time", "")),
    }

    DiscordAlert().send_alert(webhook_url=webhook, message=msg, use_embed=True)
    logger.info("Trading session start alert sent")


def should_schedule_today(minutes_before_open: int, log_dir: str | Path = "logs") -> Tuple[bool, str, PreMarketSchedule]:
    """
    - Skip weekends.
    - Skip if already sent today (sentinel file).
    - Skip if process started after the scheduled time (per your requirement).
    """
    schedule = compute_today_schedule(minutes_before_open)

    if MarketClock.is_weekend():
        return False, "weekend", schedule
    if already_sent_today(log_dir=log_dir):
        return False, "already_sent", schedule

    now = datetime.now()
    if now >= schedule.send_at:
        return False, "started_after_schedule_time", schedule

    return True, "ok", schedule

