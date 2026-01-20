import threading
import time
import json
from datetime import datetime
import sys

from variance_connect.brokers import XTS, AngelOne
from variance_connect.components import InstrumentManager

# Import logger
from utils.logger import TradingLogger, get_logger, get_component_logger

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

# Import trading components
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
from variance_connect.core.functions.instrument import create_contract_from_raw_data

# ========================================
# 1. LOAD CONFIGURATION
# ========================================

def load_config():
    """Load config and credentials from JSON files"""
    logger = get_logger("config")
    
    with open("config.json", "r") as f:
        config = json.load(f)
    
    try:
        with open("credentials.json", "r") as f:
            credentials = json.load(f)
        logger.debug("Credentials loaded successfully")
    except FileNotFoundError:
        logger.warning("credentials.json not found. Using empty credentials.")
        credentials = {}
    
    return config, credentials

# ========================================
# 2. SETUP BROKERS
# ========================================

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

# ========================================
# 3. SETUP INSTRUMENTS
# ========================================

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
        if hasattr(md_broker, 'client') and md_broker.client:
            md_broker.client.instruments = im.instruments
        broker.instruments = im.instruments
    
    # Find underlying contract (e.g., NIFTY)
    asset_name = config["underlying"]["asset_name"]
    
    try:
        underlying_contract = create_contract_from_raw_data(
            im.instruments[(im.instruments['name'] == asset_name) & (im.instruments['exchange'] == 'NSE')].iloc[0].to_dict()
        )
        logger.info(f"Found underlying contract: {underlying_contract.symbol}")
    except (IndexError, KeyError):
        logger.error(f"Underlying asset '{asset_name}' not found in instruments")
        raise Exception(f"Underlying asset '{asset_name}' not found in instruments. Please check the asset name.")
    
    return im, underlying_contract

# ========================================
# 4. SETUP TRADING COMPONENTS
# ========================================

def setup_components(config, broker, im, underlying_contract, md_streamer):
    """Initialize all trading system components"""
    
    # Configure market timing
    MarketClock.configure(
        market_open=config["market_timing"]["market_open"],
        market_close=config["market_timing"]["market_close"]
    )
    
    # Candle aggregation & strategy
    timeframe_sec = config["strategy"]["timeframe_minutes"] * 60
    candle_agg = CandleAggregator(timeframe_sec=timeframe_sec)
    strategy = EMACrossoverStrategy(
        config["strategy"]["fast_ema_period"],
        config["strategy"]["slow_ema_period"]
    )
    
    # Trading components
    # OptionSelector uses InstrumentManager which has cached instruments from Angel One REST API
    option_selector = OptionSelector(
        instrument_manager=im,
        underlying_contract=underlying_contract
    )
    risk_manager = RiskManager(broker, config)
    
    # Database & reporting
    mongo_uri = config["deployment"].get("mongo_uri", "mongodb://localhost:27017")
    db_name = config["deployment"].get("db_name", "ema_xts")
    trade_repo = TradeRepository(mongo_uri=mongo_uri, db_name=db_name)
    reporter = SessionReporter(
        webhook_url=config["deployment"].get("discord_webhook") or config["deployment"].get("discord_webhook_alerts", ""),
        trade_repo=trade_repo
    )
    
    # Trade execution
    trade_controller = TradeController(
        broker, option_selector, risk_manager,
        config, trade_repo, md_streamer
    )
    
    # Exit management (SL/TP)
    exit_manager = ExitManager(
        broker, trade_controller, risk_manager, 
        reporter, config, trade_repo
    )
    
    return {
        'candle_agg': candle_agg,
        'strategy': strategy,
        'trade_controller': trade_controller,
        'exit_manager': exit_manager,
        'reporter': reporter
    }

# ========================================
# 5. MARKET DATA HANDLERS
# ========================================

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
            symbol = self.underlying_contract.symbol if hasattr(self.underlying_contract, 'symbol') else self.config['underlying']['asset_name']
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
    
    def on_tick(self, event):
        """Process each market data tick"""
        self.tick_count += 1
        
        # Debug: Show first few ticks
        if self.tick_count <= 5:
            try:
                symbol = event.contract.symbol if event.contract and hasattr(event.contract, 'symbol') else "N/A"
                ltp = event.ltp if hasattr(event, 'ltp') else None
                self.logger.debug(f"Tick #{self.tick_count}: {symbol} | LTP: {ltp}")
            except Exception as e:
                self.logger.error(f"Error in tick debug: {e}")
        
        # Get contract symbol to check if it's underlying or option
        tick_symbol = event.contract.symbol if event.contract and hasattr(event.contract, 'symbol') else None
        underlying_symbol = self.underlying_contract.symbol if hasattr(self.underlying_contract, 'symbol') else None
        
        # Also check by token/exchange to be more robust
        tick_token = event.contract.token if event.contract and hasattr(event.contract, 'token') else None
        underlying_token = self.underlying_contract.token if hasattr(self.underlying_contract, 'token') else None
        
        # Update paper broker with live prices (for all contracts)
        if self.is_paper:
            try:
                self.broker.on_tick(event)
            except Exception as e:
                self.logger.error(f"Error in broker.on_tick: {e}", exc_info=True)
        
        # Check stop-loss and take-profit (for all contracts)
        self.components['exit_manager'].on_tick(event)
        
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
            is_underlying_tick = (tick_token == underlying_token)
        
        if is_underlying_tick:
            # Build candles from underlying ticks only
            candle = self.components['candle_agg'].on_tick(event)
            
            # Always update strategy with candle (even if None, to track progress)
            # This ensures EMA values are calculated as candles accumulate
            if candle:
                # Candle closed - process it
                # Update trailing stops on candle close (reduces noise)
                self.components['exit_manager'].on_candle_close(candle)
                
                # Only trade during market hours
                if MarketClock.is_market_open():
                    # Generate trading signals
                    signal = self.components['strategy'].on_candle(candle)
                    if signal:
                        self.strategy_logger.info(f"SIGNAL GENERATED: {signal} | Spot Price: {candle['close']:.2f} | Time: {time.strftime('%H:%M:%S')}")
                        # Execute trade based on signal
                        self.components['trade_controller'].on_signal(
                            signal=signal,
                            spot_price=candle["close"]
                        )
    
    def on_order_filled(self, event):
        """Handle order execution"""
        order_id = event.order.order_id
        trade_controller = self.components['trade_controller']
        exit_manager = self.components['exit_manager']
        logger = get_component_logger("main")
        
        # Check if this is a SL/TP order (broker orders)
        # Use list() to avoid modification during iteration
        trade_repo = self.components.get('trade_repo')
        for pos_order_id, position in list(exit_manager.positions.items()):
            if position.get("sl_order_id") == order_id:
                exit_price = event.filled_price
                # Update SL order status in database (critical: ensure status is updated)
                if trade_repo:
                    try:
                        result = trade_repo.update_order(
                            order_id=order_id,
                            status="FILLED",
                            filled_quantity=position["quantity"],
                            filled_price=exit_price
                        )
                        if result and result.matched_count > 0:
                            logger.info(f"Updated SL order {order_id} status to FILLED in database")
                        else:
                            # Order not found - try to create it (shouldn't happen, but handle gracefully)
                            logger.warning(f"SL order {order_id} not found in database, attempting to create")
                            try:
                                trade_repo.update_order(
                                    order_id=order_id,
                                    status="FILLED",
                                    filled_quantity=position["quantity"],
                                    filled_price=exit_price,
                                    upsert=True
                                )
                                logger.info(f"Created SL order {order_id} in database with FILLED status")
                            except Exception as e2:
                                logger.error(f"Failed to create SL order in database: {e2}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Failed to update SL order {order_id} status to FILLED: {e}", exc_info=True)
                        # Continue anyway - position exit should still proceed
                # Check if position is still registered (not already being closed)
                if pos_order_id in exit_manager.positions:
                    exit_manager.exit_position(pos_order_id, position, exit_price, reason="SL")
                return
            elif position.get("tp_order_id") == order_id:
                exit_price = event.filled_price
                # Update TP order status in database (critical: ensure status is updated)
                if trade_repo:
                    try:
                        result = trade_repo.update_order(
                            order_id=order_id,
                            status="FILLED",
                            filled_quantity=position["quantity"],
                            filled_price=exit_price
                        )
                        if result and result.matched_count > 0:
                            logger.info(f"Updated TP order {order_id} status to FILLED in database")
                        else:
                            # Order not found - try to create it (shouldn't happen, but handle gracefully)
                            logger.warning(f"TP order {order_id} not found in database, attempting to create")
                            try:
                                trade_repo.update_order(
                                    order_id=order_id,
                                    status="FILLED",
                                    filled_quantity=position["quantity"],
                                    filled_price=exit_price,
                                    upsert=True
                                )
                                logger.info(f"Created TP order {order_id} in database with FILLED status")
                            except Exception as e2:
                                logger.error(f"Failed to create TP order in database: {e2}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Failed to update TP order {order_id} status to FILLED: {e}", exc_info=True)
                        # Continue anyway - position exit should still proceed
                # Check if position is still registered (not already being closed)
                if pos_order_id in exit_manager.positions:
                    exit_manager.exit_position(pos_order_id, position, exit_price, reason="TP")
                return
        
        # Regular entry order
        trade_controller.on_order_filled(event)
        
        # Register position for exit monitoring (partial fills are always allowed)
        if order_id in trade_controller.open_positions:
            position = trade_controller.open_positions[order_id]
            exit_manager.register_position(position)
            
            # Subscribe to option for SL/TP monitoring
            if self.is_paper:
                option_contract = position["contract"]
                self.md_streamer.subscribe(option_contract, subscription_type="LTP")
                print(f"Subscribed to {option_contract.symbol} for SL/TP monitoring")

# ========================================
# 6. SETUP MARKET DATA STREAMER
# ========================================

def setup_market_data_streamer(config, broker, md_broker, instrument_manager=None):
    """Initialize market data streamer based on trading mode"""
    is_paper = config["deployment"]["paper_trading"]
    
    if is_paper:
        # Use Angel One market data streamer for paper trading
        if not MD_ANGEL_ONE_AVAILABLE or MD_AngelOne is None:
            raise Exception("MD_AngelOne is not available. Cannot use Angel One for market data in paper trading mode.")
        if md_broker is None:
            raise Exception("Angel One broker not connected. Cannot initialize market data streamer.")
        
        # Ensure broker has instruments set (required for streamer)
        if instrument_manager and hasattr(md_broker, 'instruments') and md_broker.instruments is None:
            md_broker.instruments = instrument_manager.instruments
        if instrument_manager and hasattr(md_broker, 'client') and md_broker.client:
            if not hasattr(md_broker.client, 'instruments') or md_broker.client.instruments is None:
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

# ========================================
# 7. HELPER FUNCTIONS
# ========================================

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
        lambda: components['reporter'].send_eod_report() if 'reporter' in components else None,
        error_msg="Failed to send EOD report",
        logger=logger
    )
    
    # Stop market data streamer
    safe_call(
        lambda: md_streamer.stop_streaming() if hasattr(md_streamer, 'stop_streaming') else None,
        error_msg="Failed to stop market data streamer",
        logger=logger
    )
    if hasattr(md_streamer, 'stop_streaming'):
        logger.info("Market data streamer stopped")

def shutdown_system(md_streamer, components, send_eod=False):
    """Gracefully shutdown the system."""
    logger = get_logger("shutdown")
    
    # Stop streamer
    safe_call(
        lambda: md_streamer.stop_streaming() if hasattr(md_streamer, 'stop_streaming') else None,
        error_msg="Failed to stop streamer during shutdown",
        logger=logger
    )
    
    # Send EOD if requested and market was open
    if send_eod and MarketClock.is_market_open():
        safe_call(
            lambda: components['reporter'].send_eod_report() if 'reporter' in components else None,
            error_msg="Failed to send final EOD report",
            logger=logger
        )

# ========================================
# 8. MAIN EXECUTION
# ========================================

def main():
    """Main trading system entry point"""
    
    # Load configuration
    config, credentials = load_config()
    
    # Setup brokers
    broker, md_broker, is_paper = setup_brokers(config, credentials)
    
    # Setup instruments
    im, underlying_contract = setup_instruments(md_broker, config, broker)
    
    # Setup market data streamer (pass instrument_manager to ensure instruments are set)
    md_streamer = setup_market_data_streamer(config, broker, md_broker, instrument_manager=im)
    
    # Setup trading components
    components = setup_components(config, broker, im, underlying_contract, md_streamer)
    
    # Setup market data handler
    handler = MarketDataHandler(config, broker, components, underlying_contract, md_streamer)
    
    # Attach callbacks
    md_streamer.attach_on_connect_handler(handler.on_connect)
    md_streamer.attach_on_disconnect_handler(handler.on_disconnect)
    md_streamer.attach_on_tick_handler(handler.on_tick)
    
    # Attach error handler if available
    if hasattr(md_streamer, 'attach_on_error_handler'):
        md_streamer.attach_on_error_handler(handler.on_error)
    
    # Setup paper trading callback
    if is_paper:
        broker.set_order_filled_callback(handler.on_order_filled)
    
    # Wait for market to open if it's currently closed
    if not MarketClock.is_market_open():
        logger = get_logger("startup")
        logger.info("Market is closed. Waiting for market to open...")
        MarketClock.wait_for_market_open(check_interval=360, verbose=True)
        logger.info("Market is now open. Starting system...")
    
    # Start market data stream
    logger = get_logger("startup")
    logger.info("Starting market data stream...")
    print("\nStarting market data stream...")
    stream_thread = threading.Thread(target=md_streamer.start_streaming, daemon=True)
    stream_thread.start()
    logger.info("Market data stream thread started")
    print("Market data stream thread started")
    time.sleep(2)
    
    # Print startup info
    trading_mode = "Paper Trading (Angel One MD)" if is_paper else "Live Trading (XTS)"
    startup_msg = f"EMA system started - {trading_mode} - Trading {config['underlying']['asset_name']} options"
    logger.info(startup_msg)
    logger.info(f"Market hours: {MarketClock.get_market_hours_str()}")
    logger.info(f"Current time: {time.strftime('%H:%M:%S')}")
    
    print(f"EMA system started - {trading_mode} - Trading {config['underlying']['asset_name']} options")
    print(f"Market hours: {MarketClock.get_market_hours_str()}")
    print(f"Current time: {time.strftime('%H:%M:%S')}")
    print(f"Waiting for market data ticks...")
    print(f"   (Ticks will appear when market is open and data is flowing)\n")
    
    # Main monitoring loop (only runs while market is open)
    last_status_time = time.time()
    status_interval = 30  # Print status every 30 seconds
    
    # Main trading loop - runs continuously, waiting for market open periods
    while True:
        try:
            # Trading loop - only runs while market is open
            while MarketClock.is_market_open():
                # Check for end-of-day square-off (safe call - continues on error)
                safe_call(
                    components['exit_manager'].check_squareoff,
                    error_msg="Error in check_squareoff",
                    logger=get_logger("error")
                )
                
                # Exit immediately if market closed after square-off
                if not MarketClock.is_market_open():
                    get_logger("eod").info("Market closed detected after square-off check")
                    break
                
                # Print status every 30 seconds
                current_time = time.time()
                if current_time - last_status_time >= status_interval:
                    strategy = components['strategy']
                    candles_count = len(strategy.candles)
                    needed_candles = strategy.slow_period
                    ema_fast = strategy.fast_ema
                    ema_slow = strategy.slow_ema
                    
                    # Format EMA values
                    if ema_fast is not None and ema_slow is not None:
                        ema_fast_str = f"{ema_fast:.2f}"
                        ema_slow_str = f"{ema_slow:.2f}"
                    else:
                        progress = f"({candles_count}/{needed_candles})" if candles_count < needed_candles else ""
                        ema_fast_str = f"N/A {progress}".strip()
                        ema_slow_str = f"N/A {progress}".strip()
                    
                    status_msg = f"Time: {time.strftime('%H:%M:%S')} | Tick: {handler.tick_count} | EMA{strategy.fast_period}: {ema_fast_str} | EMA{strategy.slow_period}: {ema_slow_str}"
                    print(status_msg)
                    get_logger("status").info(status_msg)
                    last_status_time = current_time
                
                # Sleep in chunks to check market status frequently (exit quickly when market closes)
                for _ in range(2):  # Check every 0.5 seconds
                    if not MarketClock.is_market_open():
                        break
                    time.sleep(0.5)
            
            # Market closed - process EOD and wait for next session
            if not MarketClock.is_market_open():
                process_eod(components, md_streamer)
                
                # Wait for next market open
                logger = get_logger("eod")
                logger.info("Waiting for next market open...")
                print("Waiting for next market open...")
                MarketClock.wait_for_market_open(check_interval=3600, verbose=True)
                
                # Restart market data stream
                logger.info("Market is open. Restarting market data stream...")
                print("Market is open. Restarting market data stream...")
                stream_thread = threading.Thread(target=md_streamer.start_streaming, daemon=True)
                stream_thread.start()
                logger.info("Market data stream thread restarted")
                time.sleep(1)
                last_status_time = time.time()
            
        except KeyboardInterrupt:
            print("\n\nShutting down gracefully...")
            get_logger("shutdown").info("User requested shutdown")
            shutdown_system(md_streamer, components, send_eod=True)
            sys.exit(0)
            
        except Exception as e:
            logger = get_logger("error")
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            print(f"\nUnexpected error: {e}")
            
            # If market closed, process EOD and wait for next session
            if not MarketClock.is_market_open():
                logger.info("Market is closed. Processing EOD after error.")
                process_eod(components, md_streamer, logger)
                MarketClock.wait_for_market_open(check_interval=3600, verbose=True)
                stream_thread = threading.Thread(target=md_streamer.start_streaming, daemon=True)
                stream_thread.start()
                last_status_time = time.time()
            else:
                # Market still open - wait and retry
                print("Waiting 60 seconds before retrying...")
                time.sleep(60)

# ========================================
# RUN
# ========================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        
        # Send fatal error to Discord if webhook is configured
        try:
            from reporting.discord import DiscordAlert
            with open("config.json", "r") as f:
                config = json.load(f)
            webhook_url = config.get("deployment", {}).get("discord_webhook") or config.get("deployment", {}).get("discord_webhook_alerts", "")
            if webhook_url:
                discord = DiscordAlert()
                discord.send_error_alert(
                    webhook_url=webhook_url,
                    error_type="CRITICAL",
                    error_message=f"Fatal error during startup: {str(e)}",
                    component="main",
                    traceback_str=traceback.format_exc(),
                    additional_info={"Status": "System crashed during startup"}
                )
        except:
            pass  # Don't let Discord errors prevent exit
        
        sys.exit(1)
