import threading
import time
import json
from datetime import datetime
import sys

from utils.logger import get_logger
from market.market_clock import MarketClock
from market.market_data_handler import MarketDataHandler
from app.bootstrap import (
    load_config,
    setup_brokers,
    setup_instruments,
    setup_market_data_streamer,
    setup_components,
    safe_call,
    process_eod,
    shutdown_system,
)
from reporting.pre_market import (
    should_schedule_today,
    send_pre_market_notifications,
    send_trading_session_start_alert,
)

# ========================================
# 8. MAIN EXECUTION
# ========================================

def main():
    """Main trading system entry point"""
    
    # Load configuration
    config, credentials = load_config()

    # Configure market clock early (pre-market scheduler relies on this)
    try:
        MarketClock.configure(
            market_open=config["market_timing"]["market_open"],
            market_close=config["market_timing"]["market_close"],
        )
    except Exception:
        pass
    
    # Setup brokers
    broker, md_broker, is_paper = setup_brokers(config, credentials)
    
    # Setup instruments
    im, underlying_contract = setup_instruments(md_broker, config, broker)
    
    # Setup market data streamer (pass instrument_manager to ensure instruments are set)
    md_streamer = setup_market_data_streamer(config, broker, md_broker, instrument_manager=im)

    # ------------------------------------------------
    # Pre-market notifications (T-N minutes before open)
    # - Send daily checklist + config snapshot
    # - Skip if process started after the scheduled time (per config)
    # ------------------------------------------------
    try:
        minutes_before_open = int(config.get("deployment", {}).get("discord_alerts_time_before_market_open", 30))
        ok, reason, schedule = should_schedule_today(minutes_before_open, log_dir="logs")
        logger = get_logger("startup")
        if ok:
            delay_sec = max(0.0, (schedule.send_at - datetime.now()).total_seconds())
            logger.info(
                f"Pre-market notifier scheduled at {schedule.send_at.strftime('%H:%M:%S')} "
                f"({minutes_before_open} min before open) in {delay_sec:.0f}s"
            )

            def _run_premarket():
                try:
                    time.sleep(delay_sec)
                    send_pre_market_notifications(
                        config=config,
                        credentials_loaded=bool(credentials),
                        is_paper=is_paper,
                        md_broker=md_broker,
                        instrument_manager=im,
                        underlying_contract=underlying_contract,
                        md_streamer=md_streamer,
                        log_dir="logs",
                    )
                except Exception as e:
                    logger.error(f"Pre-market notifier failed: {e}", exc_info=True)

            threading.Thread(target=_run_premarket, daemon=True).start()
        else:
            logger.info(f"Pre-market notifier not scheduled ({reason})")
    except Exception as e:
        get_logger("startup").warning(f"Failed to schedule pre-market notifier: {e}", exc_info=True)
    
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

    # One combined "trading session started" alert (single send per process start)
    try:
        send_trading_session_start_alert(
            config=config,
            is_paper=is_paper,
            broker=broker,
            md_broker=md_broker,
            instrument_manager=im,
            underlying_contract=underlying_contract,
        )
    except Exception as e:
        get_logger("startup").warning(f"Failed to send trading session start alert: {e}", exc_info=True)
    
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
