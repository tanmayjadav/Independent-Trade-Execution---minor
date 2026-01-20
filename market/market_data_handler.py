import time
from datetime import datetime

from market.market_clock import MarketClock
from reporting.discord import DiscordAlert
from utils.logger import get_component_logger


class MarketDataHandler:
    """Handles market data streaming and tick processing"""

    def __init__(self, config, broker, components, underlying_contract, md_streamer):
        self.config = config
        self.broker = broker
        self.components = components
        self.underlying_contract = underlying_contract
        self.md_streamer = md_streamer
        self.tick_count = 0
        self.is_paper = config["deployment"]["paper_trading"]

        # Initialize loggers once as instance variables
        self.logger = get_component_logger("market_data")
        self.strategy_logger = get_component_logger("strategy")

        # Discord alerts for operational issues
        self.discord = DiscordAlert()
        deployment = config.get("deployment", {}) or {}
        self.alerts_webhook = deployment.get("discord_webhook_alerts") or deployment.get("discord_webhook", "")
        self._last_discord_alert_time = 0
        self._discord_alert_interval = 60  # minimum seconds between Discord alerts for MD issues

        # Rate limiting for disconnect warnings
        self._last_disconnect_log_time = 0
        self._disconnect_log_interval = 5  # Only log once per 5 seconds
        self._disconnect_count = 0

    def on_connect(self, event):
        """Handle market data connection"""
        broker_name = "Angel One" if self.is_paper else "XTS"
        self.logger.info(f"{broker_name} Market Data Connected - Subscribing to {self.config['underlying']['asset_name']}")

        try:
            self.md_streamer.subscribe(self.underlying_contract, subscription_type="LTP")
            symbol = (
                self.underlying_contract.symbol
                if hasattr(self.underlying_contract, "symbol")
                else self.config["underlying"]["asset_name"]
            )
            self.logger.info(f"Successfully subscribed to {symbol}")
            self.logger.info(f"Waiting for market data ticks (Market hours: {MarketClock.get_market_hours_str()})")
        except Exception as e:
            self.logger.error(f"Failed to subscribe: {e}", exc_info=True)

    def on_disconnect(self, event):
        """Handle market data disconnection"""
        broker_name = "Angel One" if self.is_paper else "XTS"
        current_time = time.time()

        # Rate limit disconnect warnings to prevent spam
        # (Multiple subscriptions disconnect individually, causing many events)
        if MarketClock.is_market_open():
            self._disconnect_count += 1
            # Only log once per interval, or if it's the first disconnect
            if (current_time - self._last_disconnect_log_time) >= self._disconnect_log_interval:
                if self._disconnect_count > 1:
                    self.logger.warning(f"{broker_name} Market Data Disconnected ({self._disconnect_count} disconnect events)")
                else:
                    self.logger.warning(f"{broker_name} Market Data Disconnected")
                self._last_disconnect_log_time = current_time
                self._disconnect_count = 0

                # Send Discord alert (throttled)
                if self.alerts_webhook and (current_time - self._last_discord_alert_time) >= self._discord_alert_interval:
                    try:
                        symbol = (
                            self.underlying_contract.symbol
                            if hasattr(self.underlying_contract, "symbol")
                            else self.config["underlying"]["asset_name"]
                        )
                        msg = {
                            "title": f"{broker_name} Market Data Disconnected",
                            "color": "red",
                            "time": datetime.now().strftime("%H:%M:%S"),
                            "market_hours": MarketClock.get_market_hours_str(),
                            "underlying": symbol,
                            "note": "Streaming disconnected during market hours. Engine will continue running; investigate connectivity.",
                        }
                        self.discord.send_alert(webhook_url=self.alerts_webhook, message=msg, use_embed=True)
                        self._last_discord_alert_time = current_time
                    except Exception:
                        pass

        # Outside market hours, disconnections are expected - don't spam logs
        # Reset counter when market closes
        elif self._disconnect_count > 0:
            self._disconnect_count = 0

    def on_error(self, event):
        """Handle market data errors"""
        # Suppress errors outside market hours (they're expected)
        if not MarketClock.is_market_open():
            return

        # Only log errors during market hours
        broker_name = "Angel One" if self.is_paper else "XTS"
        error_msg = str(event) if event else "Unknown error"
        # Filter out DNS errors that occur outside market hours
        if "getaddrinfo failed" in error_msg or "11001" in error_msg:
            return  # Suppress DNS errors

        self.logger.error(f"{broker_name} Market Data Error: {error_msg}")

        # Send Discord alert (throttled)
        current_time = time.time()
        if self.alerts_webhook and (current_time - self._last_discord_alert_time) >= self._discord_alert_interval:
            try:
                symbol = (
                    self.underlying_contract.symbol
                    if hasattr(self.underlying_contract, "symbol")
                    else self.config["underlying"]["asset_name"]
                )
                msg = {
                    "title": f"{broker_name} Market Data Error",
                    "color": "red",
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "market_hours": MarketClock.get_market_hours_str(),
                    "underlying": symbol,
                    "error": error_msg[:900],
                }
                self.discord.send_alert(webhook_url=self.alerts_webhook, message=msg, use_embed=True)
                self._last_discord_alert_time = current_time
            except Exception:
                pass

    def on_tick(self, event):
        """Process each market data tick"""
        self.tick_count += 1

        # Debug: Show first few ticks
        if self.tick_count <= 5:
            try:
                symbol = event.contract.symbol if event.contract and hasattr(event.contract, "symbol") else "N/A"
                ltp = event.ltp if hasattr(event, "ltp") else None
                self.logger.debug(f"Tick #{self.tick_count}: {symbol} | LTP: {ltp}")
            except Exception as e:
                self.logger.error(f"Error in tick debug: {e}")

        # Get contract symbol to check if it's underlying or option
        tick_symbol = event.contract.symbol if event.contract and hasattr(event.contract, "symbol") else None
        underlying_symbol = self.underlying_contract.symbol if hasattr(self.underlying_contract, "symbol") else None

        # Also check by token/exchange to be more robust
        tick_token = event.contract.token if event.contract and hasattr(event.contract, "token") else None
        underlying_token = self.underlying_contract.token if hasattr(self.underlying_contract, "token") else None

        # Update paper broker with live prices (for all contracts)
        if self.is_paper:
            try:
                self.broker.on_tick(event)
            except Exception as e:
                self.logger.error(f"Error in broker.on_tick: {e}", exc_info=True)

        # Check stop-loss and take-profit (for all contracts)
        self.components["exit_manager"].on_tick(event)

        # Only build candles and generate signals from UNDERLYING contract ticks
        # This prevents option contract ticks from generating false signals
        # Check both symbol and token for more reliable matching
        is_underlying_tick = False
        if tick_symbol and underlying_symbol:
            # Direct symbol match (exact match)
            if tick_symbol == underlying_symbol:
                is_underlying_tick = True
            # Also check if it's NOT an option contract (options contain "CE" or "PE")
            elif "CE" not in tick_symbol.upper() and "PE" not in tick_symbol.upper():
                # Check token match as additional verification
                if tick_token and underlying_token and tick_token == underlying_token:
                    is_underlying_tick = True
        elif tick_token and underlying_token:
            # Fallback to token matching if symbols don't match
            is_underlying_tick = tick_token == underlying_token

        if is_underlying_tick:
            # Build candles from underlying ticks only
            candle = self.components["candle_agg"].on_tick(event)

            # Always update strategy with candle (even if None, to track progress)
            if candle:
                # Candle closed - process it
                # Update trailing stops on candle close (reduces noise)
                self.components["exit_manager"].on_candle_close(candle)

                # Only trade during market hours
                if MarketClock.is_market_open():
                    # Generate trading signals
                    signal = self.components["strategy"].on_candle(candle)
                    if signal:
                        self.strategy_logger.info(
                            f"SIGNAL GENERATED: {signal} | Spot Price: {candle['close']:.2f} | Time: {time.strftime('%H:%M:%S')}"
                        )
                        # Execute trade based on signal
                        self.components["trade_controller"].on_signal(
                            signal=signal, spot_price=candle["close"]
                        )

    def on_order_filled(self, event):
        """Handle order execution"""
        order_id = event.order.order_id
        trade_controller = self.components["trade_controller"]
        exit_manager = self.components["exit_manager"]
        logger = get_component_logger("main")

        # Prefer trade_repo from controller, fallback to components if present
        trade_repo = getattr(trade_controller, "trade_repo", None) or self.components.get("trade_repo")

        # Check if this is a SL/TP order (broker orders)
        for pos_order_id, position in list(exit_manager.positions.items()):
            if position.get("sl_order_id") == order_id:
                exit_price = event.filled_price
                # Update SL order status in database
                if trade_repo:
                    try:
                        result = trade_repo.update_order(
                            order_id=order_id,
                            status="FILLED",
                            filled_quantity=position["quantity"],
                            filled_price=exit_price,
                        )
                        if result and result.matched_count > 0:
                            logger.info(f"Updated SL order {order_id} status to FILLED in database")
                        else:
                            logger.warning(f"SL order {order_id} not found in database, attempting to create")
                            try:
                                trade_repo.update_order(
                                    order_id=order_id,
                                    status="FILLED",
                                    filled_quantity=position["quantity"],
                                    filled_price=exit_price,
                                    upsert=True,
                                )
                                logger.info(f"Created SL order {order_id} in database with FILLED status")
                            except Exception as e2:
                                logger.error(f"Failed to create SL order in database: {e2}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Failed to update SL order {order_id} status to FILLED: {e}", exc_info=True)
                if pos_order_id in exit_manager.positions:
                    exit_manager.exit_position(pos_order_id, position, exit_price, reason="SL")
                return

            if position.get("tp_order_id") == order_id:
                exit_price = event.filled_price
                # Update TP order status in database
                if trade_repo:
                    try:
                        result = trade_repo.update_order(
                            order_id=order_id,
                            status="FILLED",
                            filled_quantity=position["quantity"],
                            filled_price=exit_price,
                        )
                        if result and result.matched_count > 0:
                            logger.info(f"Updated TP order {order_id} status to FILLED in database")
                        else:
                            logger.warning(f"TP order {order_id} not found in database, attempting to create")
                            try:
                                trade_repo.update_order(
                                    order_id=order_id,
                                    status="FILLED",
                                    filled_quantity=position["quantity"],
                                    filled_price=exit_price,
                                    upsert=True,
                                )
                                logger.info(f"Created TP order {order_id} in database with FILLED status")
                            except Exception as e2:
                                logger.error(f"Failed to create TP order in database: {e2}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Failed to update TP order {order_id} status to FILLED: {e}", exc_info=True)
                if pos_order_id in exit_manager.positions:
                    exit_manager.exit_position(pos_order_id, position, exit_price, reason="TP")
                return

        # Regular entry order
        trade_controller.on_order_filled(event)

        # Register position for exit monitoring
        if order_id in trade_controller.open_positions:
            position = trade_controller.open_positions[order_id]
            exit_manager.register_position(position)

            # Subscribe to option for SL/TP monitoring
            if self.is_paper:
                option_contract = position["contract"]
                self.md_streamer.subscribe(option_contract, subscription_type="LTP")
                print(f"Subscribed to {option_contract.symbol} for SL/TP monitoring")

