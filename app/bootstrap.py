import json
from datetime import datetime
from typing import Tuple, List

from variance_connect.components import InstrumentManager
from variance_connect.core.functions.instrument import create_contract_from_raw_data

from utils.logger import get_logger

# Import market data streamers conditionally
try:
    from variance_connect.streamers.marketdata.md_xts import MD_XTS
    MD_XTS_AVAILABLE = True
except ImportError:
    MD_XTS_AVAILABLE = False
    MD_XTS = None

try:
    from variance_connect.streamers.marketdata.md_angel_one import MD_AngelOne
    MD_ANGEL_ONE_AVAILABLE = True
except ImportError:
    MD_ANGEL_ONE_AVAILABLE = False
    MD_AngelOne = None

from broker.broker import BrokerFactory
from market.candle import CandleAggregator
from market.market_clock import MarketClock
from strategy.ema_crossover import EMACrossoverStrategy
from execution.option_selector import OptionSelector
from execution.trade_controller import TradeController
from execution.exit_manager import ExitManager
from risk.risk_managment import RiskManager
from reporting.report import SessionReporter
from database.trade_repo import TradeRepository


def validate_config(config: dict) -> Tuple[bool, List[str]]:
    """
    Validate that config.json has all required fields with values.
    Returns: (is_valid, list_of_missing_fields)
    """
    missing = []
    
    # Required top-level sections
    required_sections = ["deployment", "underlying", "market_timing", "strategy", "risk", "execution"]
    for section in required_sections:
        if section not in config or not isinstance(config[section], dict):
            missing.append(f"{section} (section missing)")
            continue
        
        # deployment section
        if section == "deployment":
            required_fields = ["paper_trading"]
            for field in required_fields:
                if field not in config[section] or config[section][field] is None:
                    missing.append(f"deployment.{field}")
            
            # paper_capital required if paper_trading is True
            if config[section].get("paper_trading") is True:
                if "paper_capital" not in config[section] or config[section]["paper_capital"] is None:
                    missing.append("deployment.paper_capital")
        
        # underlying section
        elif section == "underlying":
            required_fields = ["asset_name", "strike_interval"]
            for field in required_fields:
                if field not in config[section] or not config[section][field]:
                    missing.append(f"underlying.{field}")
        
        # market_timing section
        elif section == "market_timing":
            required_fields = ["market_open", "market_close"]
            for field in required_fields:
                if field not in config[section] or not config[section][field]:
                    missing.append(f"market_timing.{field}")
        
        # strategy section
        elif section == "strategy":
            required_fields = ["fast_ema_period", "slow_ema_period", "timeframe_minutes"]
            for field in required_fields:
                if field not in config[section] or config[section][field] is None:
                    missing.append(f"strategy.{field}")
        
        # risk section
        elif section == "risk":
            required_fields = ["mode", "value", "allow_multiple_positions", "max_daily_loss", "max_daily_loss_percent"]
            for field in required_fields:
                if field not in config[section] or config[section][field] is None:
                    missing.append(f"risk.{field}")
        
        # execution section
        elif section == "execution":
            required_fields = [
                "order_type", "sl_percent", "tp_percent", "squareoff_time",
                "trailing_sl", "breakeven_enabled", "breakeven_trigger_percent",
                "sl_update_threshold_percent"
            ]
            for field in required_fields:
                if field not in config[section] or config[section][field] is None:
                    missing.append(f"execution.{field}")
    
    return len(missing) == 0, missing


def validate_credentials(credentials: dict, is_paper: bool) -> Tuple[bool, List[str]]:
    """
    Validate that credentials.json has all required fields with values.
    Returns: (is_valid, list_of_missing_fields)
    """
    missing = []
    
    if is_paper:
        # Paper trading requires Angel One credentials
        required_fields = ["client_code", "api_key", "password", "totp_key"]
        for field in required_fields:
            if field not in credentials or not credentials[field]:
                missing.append(f"credentials.{field}")
    else:
        # Live trading requires XTS credentials (adjust based on your broker)
        # For now, we'll check for common fields
        required_fields = ["user_id", "api_key", "api_secret"]
        for field in required_fields:
            if field not in credentials or not credentials[field]:
                missing.append(f"credentials.{field}")
    
    return len(missing) == 0, missing


def load_config():
    """Load config and credentials from JSON files with validation"""
    logger = get_logger("config")

    # Load config
    try:
        with open("config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("config.json not found")
        raise Exception("config.json file not found")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config.json: {e}")
        raise Exception(f"Invalid JSON in config.json: {e}")
    
    # Validate config
    config_valid, config_missing = validate_config(config)
    if not config_valid:
        logger.error(f"Config validation failed. Missing fields: {', '.join(config_missing)}")
        raise Exception(f"Config validation failed. Missing required fields: {', '.join(config_missing)}")
    
    logger.info("Config loaded and validated successfully")
    
    # Load credentials
    try:
        with open("credentials.json", "r") as f:
            credentials = json.load(f)
        logger.debug("Credentials loaded successfully")
    except FileNotFoundError:
        logger.warning("credentials.json not found. Using empty credentials.")
        credentials = {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in credentials.json: {e}")
        credentials = {}
    
    # Validate credentials (only if not empty)
    credentials_valid = True
    credentials_missing = []
    if credentials:
        is_paper = config.get("deployment", {}).get("paper_trading", False)
        credentials_valid, credentials_missing = validate_credentials(credentials, is_paper)
        if not credentials_valid:
            logger.warning(f"Credentials validation failed. Missing fields: {', '.join(credentials_missing)}")
    else:
        # If credentials file not found, mark as invalid
        credentials_valid = False
        credentials_missing = ["credentials.json file not found"]
    
    return config, credentials, config_valid, credentials_valid, config_missing, credentials_missing


def setup_brokers(config, credentials):
    """Initialize trading and market data brokers"""
    logger = get_logger("broker")
    is_paper = config["deployment"]["paper_trading"]

    # Create both brokers
    broker, md_broker = BrokerFactory.create(config, credentials)

    # Connect brokers
    if is_paper:
        # Paper trading: connect market data broker (Angel One)
        try:
            md_broker.connect()
            logger.info("Connected to Angel One for market data (paper trading mode)")
        except Exception as e:
            logger.error(f"Failed to connect to Angel One for market data: {e}")
            logger.warning("Paper trading will continue but may not have live market data")
            md_broker = None
    else:
        # Live trading: connect trading broker (same as market data broker)
        broker.connect()
        logger.info("Live broker connected")

    return broker, md_broker, is_paper


def setup_instruments(md_broker, config, broker):
    """Fetch and configure trading instruments"""
    logger = get_logger("instruments")
    im = InstrumentManager()
    im.fetch_base_instruments()

    # Fetch broker-specific instruments
    if md_broker:
        try:
            instruments_df = md_broker.get_instruments()
            if instruments_df is not None and not instruments_df.empty:
                im.add_broker_instruments(md_broker.BROKER, instruments_df)
                logger.info(f"Loaded {len(instruments_df)} instruments from {md_broker.BROKER}")
            else:
                logger.warning("get_instruments() returned empty or None. Using base instruments only.")
        except Exception as e:
            logger.error(f"Failed to fetch instruments from broker: {e}", exc_info=True)
            if not config["deployment"]["paper_trading"]:
                raise  # Fail for live trading

        # Always set instruments on broker (even if fetch failed, use base instruments)
        # This is required for market data streamer to work
        md_broker.instruments = im.instruments
        if hasattr(md_broker, "client") and md_broker.client:
            md_broker.client.instruments = im.instruments
        broker.instruments = im.instruments

    # Find underlying contract (e.g., NIFTY)
    asset_name = config["underlying"]["asset_name"]

    try:
        underlying_contract = create_contract_from_raw_data(
            im.instruments[(im.instruments["name"] == asset_name) & (im.instruments["exchange"] == "NSE")]
            .iloc[0]
            .to_dict()
        )
        logger.info(f"Found underlying contract: {underlying_contract.symbol}")
    except (IndexError, KeyError):
        logger.error(f"Underlying asset '{asset_name}' not found in instruments")
        raise Exception(f"Underlying asset '{asset_name}' not found in instruments. Please check the asset name.")

    return im, underlying_contract


def setup_components(config, broker, im, underlying_contract, md_streamer):
    """Initialize all trading system components"""

    # Configure market timing
    MarketClock.configure(
        market_open=config["market_timing"]["market_open"],
        market_close=config["market_timing"]["market_close"],
    )

    # Candle aggregation & strategy
    timeframe_sec = config["strategy"]["timeframe_minutes"] * 60
    candle_agg = CandleAggregator(timeframe_sec=timeframe_sec)
    strategy = EMACrossoverStrategy(
        config["strategy"]["fast_ema_period"], config["strategy"]["slow_ema_period"]
    )

    # Trading components
    option_selector = OptionSelector(
        instrument_manager=im, underlying_contract=underlying_contract
    )
    risk_manager = RiskManager(broker, config)

    # Database & reporting
    mongo_uri = config["deployment"].get("mongo_uri", "mongodb://localhost:27017")
    db_name = config["deployment"].get("db_name", "ema_xts")
    trade_repo = TradeRepository(mongo_uri=mongo_uri, db_name=db_name)
    reporter = SessionReporter(
        webhook_url=config["deployment"].get("discord_webhook")
        or config["deployment"].get("discord_webhook_alerts", ""),
        trade_repo=trade_repo,
    )

    # Trade execution
    trade_controller = TradeController(
        broker, option_selector, risk_manager, config, trade_repo, md_streamer
    )

    # Exit management (SL/TP)
    exit_manager = ExitManager(
        broker, trade_controller, risk_manager, reporter, config, trade_repo
    )

    return {
        "candle_agg": candle_agg,
        "strategy": strategy,
        "trade_controller": trade_controller,
        "exit_manager": exit_manager,
        "reporter": reporter,
    }


def setup_market_data_streamer(config, broker, md_broker, instrument_manager=None):
    """Initialize market data streamer based on trading mode"""
    is_paper = config["deployment"]["paper_trading"]

    if is_paper:
        # Use Angel One market data streamer for paper trading
        if not MD_ANGEL_ONE_AVAILABLE or MD_AngelOne is None:
            raise Exception(
                "MD_AngelOne is not available. Cannot use Angel One for market data in paper trading mode."
            )
        if md_broker is None:
            raise Exception("Angel One broker not connected. Cannot initialize market data streamer.")

        # Ensure broker has instruments set (required for streamer)
        if instrument_manager and hasattr(md_broker, "instruments") and md_broker.instruments is None:
            md_broker.instruments = instrument_manager.instruments
        if instrument_manager and hasattr(md_broker, "client") and md_broker.client:
            if not hasattr(md_broker.client, "instruments") or md_broker.client.instruments is None:
                md_broker.client.instruments = instrument_manager.instruments

        md_streamer = MD_AngelOne(md_broker)
        logger = get_logger("market_data")
        logger.info("Using Angel One market data streamer for paper trading")
    else:
        # Use XTS market data streamer for live trading
        if not MD_XTS_AVAILABLE or MD_XTS is None:
            raise Exception("MD_XTS is not available. Cannot use XTS for market data in live trading mode.")
        md_streamer = MD_XTS(broker)
        logger = get_logger("market_data")
        logger.info("Using XTS market data streamer for live trading")

    return md_streamer


def safe_call(func, *args, error_msg="", logger=None, **kwargs):
    """Safely call a function, logging errors but not raising exceptions."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if logger:
            logger.error(f"{error_msg}: {e}", exc_info=True)
        return None


def process_eod(components, md_streamer, logger=None):
    """Process end-of-day: send report and stop streamer."""
    if logger is None:
        logger = get_logger("eod")

    current_time_str = datetime.now().strftime("%H:%M:%S")
    logger.info(f"Market closed at {current_time_str}. Sending end-of-day report...")
    print(f"\nMarket closed at {current_time_str}. Sending end-of-day report...")

    # Send EOD report
    safe_call(
        lambda: components["reporter"].send_eod_report() if "reporter" in components else None,
        error_msg="Failed to send EOD report",
        logger=logger,
    )

    # Stop market data streamer
    safe_call(
        lambda: md_streamer.stop_streaming() if hasattr(md_streamer, "stop_streaming") else None,
        error_msg="Failed to stop market data streamer",
        logger=logger,
    )
    if hasattr(md_streamer, "stop_streaming"):
        logger.info("Market data streamer stopped")


def shutdown_system(md_streamer, components, send_eod=False):
    """Gracefully shutdown the system."""
    logger = get_logger("shutdown")

    # 0) Stop taking new trades immediately (best-effort)
    try:
        tc = components.get("trade_controller")
        if tc is not None and hasattr(tc, "trading_enabled"):
            tc.trading_enabled = False
    except Exception:
        pass

    # 1) Close all open positions at market price (best-effort)
    try:
        em = components.get("exit_manager")
        if em is not None and hasattr(em, "close_all_positions"):
            em.close_all_positions(reason="SYSTEM_SHUTDOWN")
    except Exception as e:
        logger.warning(f"Failed to force-close open positions during shutdown: {e}", exc_info=True)

    # Stop streamer
    safe_call(
        lambda: md_streamer.stop_streaming() if hasattr(md_streamer, "stop_streaming") else None,
        error_msg="Failed to stop streamer during shutdown",
        logger=logger,
    )

    # Send EOD if requested and market was open
    if send_eod and MarketClock.is_market_open():
        safe_call(
            lambda: components["reporter"].send_eod_report() if "reporter" in components else None,
            error_msg="Failed to send final EOD report",
            logger=logger,
        )

