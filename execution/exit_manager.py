from datetime import datetime
from variance_connect.utils.enums import (
    Variety,
    TradeAction,
    OrderType,
    ProductType,
    TimeInForce
)
from utils.logger import get_component_logger


class ExitManager:
    """
    Handles SL, TP and time-based exits for open positions.
    """

    def __init__(
        self,
        broker,
        trade_controller,
        risk_manager,
        reporter,
        config: dict,
        trade_repo=None
    ):
        self.broker = broker
        self.trade_controller = trade_controller
        self.risk_manager = risk_manager
        self.reporter = reporter
        self.trade_repo = trade_repo
        self.config = config
        self.logger = get_component_logger("exit_manager")

        self.positions = {}   # order_id -> position dict

        self.sl_pct = config["execution"]["sl_percent"]
        self.tp_pct = config["execution"]["tp_percent"]
        self.squareoff_time = config["execution"]["squareoff_time"]
        
        # TP exit configuration
        self.tp_exit_enabled = config["execution"].get("tp_exit_enabled", True)
        
        # Trailing stop configuration
        self.trailing_sl = config["execution"].get("trailing_sl", False)
        self.breakeven_enabled = config["execution"].get("breakeven_enabled", False)
        self.breakeven_trigger_pct = config["execution"].get("breakeven_trigger_percent", self.tp_pct)
        
        # Broker stop-loss orders
        self.use_broker_sl_orders = config["execution"].get("use_broker_sl_orders", True)
        self.sl_update_threshold = config["execution"].get("sl_update_threshold_percent", 1.0)

    # -------------------------------------------------
    # Position lifecycle
    # -------------------------------------------------

    def register_position(self, position: dict):
        """
        Called once order is filled.
        Initializes SL/TP and tracking variables.
        Places broker stop-loss orders if enabled.
        """
        entry_price = position.get("entry_price")
        
        # Validate entry_price is set
        if entry_price is None or entry_price <= 0:
            self.logger.error(f"Cannot register position {position.get('order_id')}: entry_price is None or invalid")
            raise ValueError(f"entry_price must be set and > 0 when registering position. Got: {entry_price}")

        # Initial SL/TP prices
        position["sl_price"] = entry_price * (1 - self.sl_pct / 100)
        # TP price: only set if TP exit is enabled, otherwise set to very high value (effectively disabled)
        if self.tp_exit_enabled:
            position["tp_price"] = entry_price * (1 + self.tp_pct / 100)
        else:
            position["tp_price"] = float('inf')  # Effectively disables TP exit
        
        # Trailing stop tracking
        position["highest_price"] = entry_price  # Track highest price reached
        position["lowest_price"] = entry_price   # Track lowest price reached
        position["breakeven_triggered"] = False   # Track if breakeven SL is set
        
        # Store original entry for breakeven calculation
        position["entry_price_original"] = entry_price
        
        # Ensure entry_price is set (defensive check)
        if position.get("entry_price") is None:
            position["entry_price"] = entry_price
        
        # Broker stop-loss order tracking
        position["sl_order_id"] = None
        position["tp_order_id"] = None
        position["last_sl_price"] = position["sl_price"]  # Track last SL price for updates

        self.positions[position["order_id"]] = position
        
        # Place broker stop-loss and take-profit orders
        if self.use_broker_sl_orders:
            self._place_broker_sl_order(position)
            # Only place TP order if TP exit is enabled
            if self.tp_exit_enabled:
                self._place_broker_tp_order(position)

    def deregister_position(self, order_id: str):
        """Deregister position and cancel any pending broker orders."""
        if order_id in self.positions:
            pos = self.positions[order_id]
            
            # Cancel broker stop-loss order if exists
            if self.use_broker_sl_orders and pos.get("sl_order_id"):
                try:
                    if hasattr(self.broker, 'cancel_order'):
                        self.broker.cancel_order(pos["sl_order_id"])
                except Exception as e:
                    self.logger.warning(f"Failed to cancel SL order {pos['sl_order_id']}: {e}")
            
            # Cancel broker take-profit order if exists
            if self.use_broker_sl_orders and pos.get("tp_order_id"):
                try:
                    if hasattr(self.broker, 'cancel_order'):
                        self.broker.cancel_order(pos["tp_order_id"])
                except Exception as e:
                    self.logger.warning(f"Failed to cancel TP order {pos['tp_order_id']}: {e}")
        
        self.positions.pop(order_id, None)

    # -------------------------------------------------
    # Market data hook (option ticks)
    # -------------------------------------------------

    def on_tick(self, event):
        """
        Called on EVERY option tick.
        Handles immediate SL/TP checks (hard stops) only.
        Trailing stop updates are handled in on_candle_close().
        """
        ltp = float(event.ltp)
        token = event.contract.token

        for order_id, pos in list(self.positions.items()):
            if pos["contract"].token != token:
                continue

            # Update highest/lowest prices for trailing stops (tracking only)
            if ltp > pos["highest_price"]:
                pos["highest_price"] = ltp
            if ltp < pos["lowest_price"]:
                pos["lowest_price"] = ltp

            # Check exit conditions (hard stops only - immediate execution)
            # If using broker orders, broker will execute automatically
            if not self.use_broker_sl_orders:
                if ltp <= pos["sl_price"]:
                    self._exit_position_internal(order_id, pos, ltp, reason="SL")
                    continue

                # TP exit check: only if TP exit is enabled
                if self.tp_exit_enabled and ltp >= pos["tp_price"]:
                    self._exit_position_internal(order_id, pos, ltp, reason="TP")

    # -------------------------------------------------
    # Candle-close based trailing stop updates
    # -------------------------------------------------

    def on_candle_close(self, candle):
        """
        Called when a candle closes (from underlying).
        Updates trailing SL/TP and breakeven logic based on candle close price.
        This reduces noise compared to tick-by-tick updates.
        """
        # Get current LTP for each option position and update trailing stops
        for order_id, pos in list(self.positions.items()):
            try:
                # Get current option price from broker
                ltp = self.broker.get_ltp(pos["contract"])
                if ltp <= 0:
                    continue  # Skip if LTP not available
                
                ltp = float(ltp)
                
                # Update highest/lowest prices (in case candle close missed some ticks)
                if ltp > pos["highest_price"]:
                    pos["highest_price"] = ltp
                if ltp < pos["lowest_price"]:
                    pos["lowest_price"] = ltp

                sl_updated = False

                # Breakeven: Move SL to entry price once profit threshold is reached
                # Breakeven takes precedence over trailing SL
                if self.breakeven_enabled and not pos["breakeven_triggered"]:
                    profit_pct = ((ltp - pos["entry_price_original"]) / pos["entry_price_original"]) * 100
                    if profit_pct >= self.breakeven_trigger_pct:
                        pos["sl_price"] = pos["entry_price_original"]  # Move SL to breakeven
                        pos["breakeven_triggered"] = True
                        sl_updated = True
                        self.logger.info(f"Breakeven SL activated at entry price: {pos['entry_price_original']:.2f} for {pos['contract'].symbol}")

                # Trailing SL: Move SL up as price moves favorably (but never down)
                # Only applies if breakeven hasn't been triggered yet
                if self.trailing_sl and not pos["breakeven_triggered"]:
                    new_sl = ltp * (1 - self.sl_pct / 100)
                    if new_sl > pos["sl_price"]:
                        pos["sl_price"] = new_sl
                        sl_updated = True
                        self.logger.debug(f"Trailing SL updated: {pos['sl_price']:.2f} for {pos['contract'].symbol}")

                # Update broker stop-loss order if SL changed significantly
                if self.use_broker_sl_orders and sl_updated:
                    sl_change_pct = abs((pos["sl_price"] - pos["last_sl_price"]) / pos["last_sl_price"]) * 100
                    if sl_change_pct >= self.sl_update_threshold:
                        self._update_broker_sl_order(pos)
                        
            except Exception as e:
                self.logger.error(f"Error updating trailing stops for position {order_id}: {e}", exc_info=True)

    # -------------------------------------------------
    # Time-based square-off
    # -------------------------------------------------

    def check_squareoff(self):
        """
        Called periodically from main loop.
        """
        now = datetime.now().strftime("%H:%M")
        if now < self.squareoff_time:
            return

        for order_id, pos in list(self.positions.items()):
            ltp = self.broker.get_ltp(pos["contract"])
            if ltp > 0:
                self._exit_position_internal(order_id, pos, ltp, reason="SQUAREOFF")

    # -------------------------------------------------
    # Internal helpers
    # -------------------------------------------------

    def exit_position(
        self,
        order_id: str,
        position: dict,
        exit_price: float,
        reason: str
    ):
        """
        Public method to exit position (called from broker order execution).
        """
        self._exit_position_internal(order_id, position, exit_price, reason)
    
    def _exit_position_internal(
        self,
        order_id: str,
        position: dict,
        exit_price: float,
        reason: str
    ):
        """
        Internal implementation of exit position logic.
        """
        
        # Prevent duplicate exits - check if position is already being closed
        if order_id not in self.positions:
            self.logger.warning(f"Position {order_id} already closed or not registered. Skipping exit.")
            return

        # Place exit order only if not using broker orders
        # (Broker orders execute automatically, so we don't need to place another order)
        if not self.use_broker_sl_orders:
            self.broker.place_order(
                contract=position["contract"],
                variety=Variety.REGULAR,
                trade_action=TradeAction.SELL,
                quantity=position["quantity"],
                disclosed_quantity=0,
                order_type=OrderType.MARKET,
                price=0.0,
                trigger_price=0.0,
                product_type=ProductType.MIS,
                time_in_force=TimeInForce.DAY,
            )

        # Get entry_price with fallback logic
        entry_price = position.get("entry_price")
        if entry_price is None:
            # Try to get from entry_price_original (set during register_position)
            entry_price = position.get("entry_price_original")
            if entry_price is None:
                # Try to get from trade_controller's open_positions
                if hasattr(self.trade_controller, 'open_positions') and order_id in self.trade_controller.open_positions:
                    entry_price = self.trade_controller.open_positions[order_id].get("entry_price")
                
                # If still None, try to get from database
                if entry_price is None and self.trade_repo:
                    try:
                        pos_doc = self.trade_repo.positions.find_one({"symbol": position["contract"].symbol, "status": "OPEN"})
                        if pos_doc:
                            entry_price = pos_doc.get("entry_price")
                    except Exception as e:
                        self.logger.warning(f"Failed to retrieve entry_price from database: {e}")
                
                # Last resort: use exit_price (results in 0 PnL, but prevents crash)
                if entry_price is None:
                    self.logger.error(f"entry_price is None for order {order_id}. Using exit_price as fallback (PnL will be 0)")
                    entry_price = exit_price
        
        # Calculate PnL
        pnl = (exit_price - entry_price) * position["quantity"]

        # Get exit order_id if this was triggered by a broker order (SL/TP)
        exit_order_id = order_id  # Default to entry order_id
        if reason in ("SL", "TP"):
            # Try to find the exit order_id from position
            if reason == "SL" and position.get("sl_order_id"):
                exit_order_id = position["sl_order_id"]
            elif reason == "TP" and position.get("tp_order_id"):
                exit_order_id = position["tp_order_id"]
        
        # Save trade and update position in database
        if self.trade_repo:
            try:
                # Save EXIT trade
                self.trade_repo.save_trade(
                    order_id=exit_order_id,  # Use exit order_id for EXIT trades
                    trade_type="EXIT",
                    price=exit_price,
                    quantity=position["quantity"],
                    pnl=pnl,
                    reason=reason,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    fill_number=1
                )
                self.logger.info(f"Saved EXIT trade to database: Exit Order {exit_order_id} | Entry Order {order_id} | Qty: {position['quantity']} | Entry: {entry_price:.2f} | Exit: {exit_price:.2f} | PnL: {pnl:.2f} | Reason: {reason}")
                
                # Update position status to CLOSED
                # Ensure entry_price is set in position dict before saving
                position["exit_price"] = exit_price
                if position.get("entry_price") is None:
                    position["entry_price"] = entry_price
                self.trade_repo.upsert_position(position, status="CLOSED")
                self.logger.info(f"Updated position in database: {order_id} | Status: CLOSED | Entry: {entry_price:.2f}")
            except Exception as e:
                self.logger.error(f"Failed to save trade to database: {e}", exc_info=True)

        # Notify controllers
        self.trade_controller.on_order_exit(order_id)
        self.risk_manager.on_position_closed(
            order_id=order_id,
            exit_price=exit_price,
            quantity=position["quantity"],
            entry_price=entry_price  # Pass the calculated entry_price
        )
        self.reporter.on_trade_closed(pnl)

        # Cancel broker orders if they exist (they may have already executed)
        if self.use_broker_sl_orders:
            if position.get("sl_order_id"):
                try:
                    if hasattr(self.broker, 'cancel_order'):
                        self.broker.cancel_order(position["sl_order_id"])
                except:
                    pass  # Order may have already executed
            
            if position.get("tp_order_id"):
                try:
                    if hasattr(self.broker, 'cancel_order'):
                        self.broker.cancel_order(position["tp_order_id"])
                except:
                    pass  # Order may have already executed

        # Cleanup
        self.deregister_position(order_id)
    
    # -------------------------------------------------
    # Broker stop-loss order management
    # -------------------------------------------------
    
    def _place_broker_sl_order(self, position: dict):
        """Place stop-loss order with broker."""
        try:
            sl_order_id = self.broker.place_order(
                contract=position["contract"],
                variety=Variety.REGULAR,
                trade_action=TradeAction.SELL,
                quantity=position["quantity"],
                disclosed_quantity=0,
                order_type=OrderType.STOP,  # Stop-loss order
                price=0.0,
                trigger_price=position["sl_price"],  # Trigger price for stop-loss
                product_type=ProductType.MIS,
                time_in_force=TimeInForce.DAY,
            )
            position["sl_order_id"] = sl_order_id
            
            # Save SL order to database
            if self.trade_repo:
                try:
                    is_paper = self.config.get("deployment", {}).get("paper_trading", False)
                    if is_paper:
                        order_doc = {
                            "order_id": sl_order_id,
                            "symbol": position["contract"].symbol if hasattr(position["contract"], 'symbol') else "N/A",
                            "contract": position["contract"].symbol if hasattr(position["contract"], 'symbol') else "N/A",
                            "quantity": position["quantity"],
                            "order_type": "STOP",
                            "signal": "SL",  # Mark as stop-loss order
                            "price": 0.0,
                            "trigger_price": position["sl_price"],
                            "status": "PENDING",
                            "timestamp": datetime.utcnow(),
                            "paper_trading": True,
                            "entry_order_id": position.get("order_id")  # Link to entry order
                        }
                        self.trade_repo.orders.insert_one(order_doc)
                        self.logger.info(f"Saved SL order to database: {sl_order_id} | Trigger: {position['sl_price']:.2f}")
                except Exception as e:
                    self.logger.error(f"Failed to save SL order to database: {e}", exc_info=True)
            
            print(f"Broker stop-loss order placed: {sl_order_id} @ ₹{position['sl_price']:.2f}")
        except Exception as e:
            print(f"Failed to place broker stop-loss order: {e}")
            # Fallback to software monitoring
            self.use_broker_sl_orders = False
    
    def _place_broker_tp_order(self, position: dict):
        """Place take-profit order with broker (using limit order)."""
        try:
            tp_order_id = self.broker.place_order(
                contract=position["contract"],
                variety=Variety.REGULAR,
                trade_action=TradeAction.SELL,
                quantity=position["quantity"],
                disclosed_quantity=0,
                order_type=OrderType.LIMIT,  # Take-profit as limit order
                price=position["tp_price"],  # Limit price for take-profit
                trigger_price=0.0,
                product_type=ProductType.MIS,
                time_in_force=TimeInForce.DAY,
            )
            position["tp_order_id"] = tp_order_id
            
            # Save TP order to database
            if self.trade_repo:
                try:
                    is_paper = self.config.get("deployment", {}).get("paper_trading", False)
                    if is_paper:
                        order_doc = {
                            "order_id": tp_order_id,
                            "symbol": position["contract"].symbol if hasattr(position["contract"], 'symbol') else "N/A",
                            "contract": position["contract"].symbol if hasattr(position["contract"], 'symbol') else "N/A",
                            "quantity": position["quantity"],
                            "order_type": "LIMIT",
                            "signal": "TP",  # Mark as take-profit order
                            "price": position["tp_price"],
                            "status": "PENDING",
                            "timestamp": datetime.utcnow(),
                            "paper_trading": True,
                            "entry_order_id": position.get("order_id")  # Link to entry order
                        }
                        self.trade_repo.orders.insert_one(order_doc)
                        self.logger.info(f"Saved TP order to database: {tp_order_id} | Price: {position['tp_price']:.2f}")
                except Exception as e:
                    self.logger.error(f"Failed to save TP order to database: {e}", exc_info=True)
            
            print(f"Broker take-profit order placed: {tp_order_id} @ ₹{position['tp_price']:.2f}")
        except Exception as e:
            print(f"Failed to place broker take-profit order: {e}")
    
    def _update_broker_sl_order(self, position: dict):
        """Update broker stop-loss order when trailing SL moves."""
        if not position.get("sl_order_id"):
            return
        
        try:
            # Cancel old stop-loss order
            if hasattr(self.broker, 'cancel_order'):
                self.broker.cancel_order(position["sl_order_id"])
            
            # Place new stop-loss order at updated price
            new_sl_order_id = self.broker.place_order(
                contract=position["contract"],
                variety=Variety.REGULAR,
                trade_action=TradeAction.SELL,
                quantity=position["quantity"],
                disclosed_quantity=0,
                order_type=OrderType.STOP,
                price=0.0,
                trigger_price=position["sl_price"],
                product_type=ProductType.MIS,
                time_in_force=TimeInForce.DAY,
            )
            position["sl_order_id"] = new_sl_order_id
            position["last_sl_price"] = position["sl_price"]
            print(f"Broker stop-loss order updated: {new_sl_order_id} @ ₹{position['sl_price']:.2f}")
        except Exception as e:
            print(f"Failed to update broker stop-loss order: {e}")
