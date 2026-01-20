# execution/trade_controller.py

from variance_connect.utils.enums import (
    Variety,
    TradeAction,
    OrderType,
    ProductType,
    TimeInForce
)
from utils.logger import get_component_logger
from datetime import datetime
import threading
import time


class OrderStatus:
    """Order status constants for tracking order lifecycle."""
    PENDING = "PENDING"           # Order placed, waiting for fill
    PARTIAL = "PARTIAL"           # Partially filled
    FILLED = "FILLED"             # Fully filled
    CANCELLED = "CANCELLED"       # Order cancelled (timeout or manual)
    REJECTED = "REJECTED"         # Order rejected by broker
    EXPIRED = "EXPIRED"           # Order expired (time-based)


class TradeController:
    """
    Central execution controller.
    Handles signal → order placement.
    """

    def __init__(
        self,
        broker,
        option_selector,
        risk_manager,
        config: dict,
        trade_repo=None,
        md_streamer=None
    ):
        self.broker = broker
        self.option_selector = option_selector
        self.risk_manager = risk_manager
        self.trade_repo = trade_repo
        self.md_streamer = md_streamer

        self.config = config
        self.logger = get_component_logger("execution")

        self.open_positions = {}   # order_id → position dict
        self.pending_orders = {}   # order_id → order details for timeout tracking
        self.trading_enabled = True
        
        # Execution config
        exec_config = config.get("execution", {})
        self.order_type = OrderType.MARKET if exec_config.get("order_type", "MARKET") == "MARKET" else OrderType.LIMIT
        self.price_tolerance_pct = exec_config.get("price_tolerance_percent", 2.0)
        self.order_timeout = exec_config.get("order_timeout_seconds", 30)

    # -------------------------------------------------
    # Public API
    # -------------------------------------------------

    def on_signal(self, signal: str, spot_price: float):
        """
        Called by main.py when EMA strategy emits a signal.
        """

        if not self.trading_enabled:
            self.logger.warning(f"Trading disabled - ignoring signal {signal}")
            return

        if signal not in ("BUY_CE", "BUY_PE"):
            self.logger.warning(f"Invalid signal: {signal}")
            return

        if not self.risk_manager.can_take_new_trade():
            self.logger.warning(f"Risk manager blocked trade for signal {signal}")
            return

        option_contract = self.option_selector.select(
            signal=signal,
            spot_price=spot_price
        )

        if not option_contract:
            self.logger.warning(f"No valid option contract found for signal {signal} at spot {spot_price:.2f}")
            return
        
        symbol = option_contract.symbol if hasattr(option_contract, 'symbol') else 'N/A'
        self.logger.info(f"Option selected: {symbol} | Signal: {signal} | Spot: {spot_price:.2f}")

        # Subscribe to option contract for market data (needed for LTP)
        if self.md_streamer:
            try:
                self.md_streamer.subscribe(option_contract, subscription_type="LTP")
                symbol = option_contract.symbol if hasattr(option_contract, 'symbol') else 'N/A'
                self.logger.info(f"Subscribed to {symbol} for LTP")
                # Give subscription time to establish and receive first tick
                time.sleep(0.5)  # Wait 500ms for subscription to establish
            except Exception as e:
                symbol = option_contract.symbol if hasattr(option_contract, 'symbol') else 'N/A'
                self.logger.warning(f"Failed to subscribe to {symbol}: {e}", exc_info=True)

        # Wait for LTP to be available (market data ticks need time to arrive)
        # Wait up to 15 seconds for LTP to arrive
        max_retries = 15  # 15 retries * 1s = 15 seconds
        retry_count = 0
        entry_price = 0.0
        
        symbol = option_contract.symbol if hasattr(option_contract, 'symbol') else 'N/A'
        contract_token = option_contract.token if hasattr(option_contract, 'token') else 'N/A'
        
        while retry_count < max_retries:
            entry_price = self.broker.get_ltp(option_contract)
            if entry_price > 0:
                self.logger.info(f"LTP obtained for {symbol} after {retry_count + 1} retries: {entry_price:.2f}")
                break
            
            time.sleep(1)  # 1 second per retry
            retry_count += 1
        
        # Skip trade if no LTP after 15 seconds
        if entry_price <= 0:
            self.logger.warning(f"LTP not available for {symbol} (token: {contract_token}) after {max_retries} retries (waited 15.0s)")
            self.logger.warning(f"  Skipping trade - no LTP available")
            self.logger.warning(f"  This may happen if:")
            self.logger.warning(f"  - Option contract is not actively traded (no trades = no ticks)")
            self.logger.warning(f"  - Market data subscription is delayed (Angel One may take time)")
            self.logger.warning(f"  - Contract symbol/token mismatch (check if subscription succeeded)")
            return

        # Update broker's LTP cache with the price we got
        # This ensures PaperBroker.place_order() can find the LTP and fill immediately
        is_paper = self.config.get("deployment", {}).get("paper_trading", False)
        if is_paper and hasattr(self.broker, 'ltp_cache'):
            if contract_token:
                self.broker.ltp_cache[contract_token] = entry_price

        quantity = self.risk_manager.calculate_quantity(
            entry_price=entry_price,
            contract=option_contract
        )

        if quantity <= 0:
            available_capital = self.risk_manager.get_available_capital()
            self.logger.warning(f"Invalid quantity calculated: {quantity} (entry_price: {entry_price:.2f}, capital: {available_capital:.2f})")
            return
        
        symbol = option_contract.symbol if hasattr(option_contract, 'symbol') else 'N/A'
        self.logger.info(f"Placing order: {signal} | {symbol} | Qty: {quantity} | Price: {entry_price:.2f}")

        # Calculate limit price if using LIMIT orders
        limit_price = None
        if self.order_type == OrderType.LIMIT:
            # Set limit price with tolerance
            limit_price = entry_price * (1 + self.price_tolerance_pct / 100)

        self._place_entry_order(
            contract=option_contract,
            quantity=quantity,
            signal=signal,
            limit_price=limit_price
        )

    # -------------------------------------------------
    # Internal helpers
    # -------------------------------------------------

    def _place_entry_order(self, contract, quantity: int, signal: str, limit_price: float = None):
        """
        Places BUY order (CE or PE).
        Supports both MARKET and LIMIT orders.
        """
        order_price = limit_price if self.order_type == OrderType.LIMIT else 0.0
        is_paper = self.config.get("deployment", {}).get("paper_trading", False)

        # CRITICAL: Generate order_id and save to database BEFORE placing order
        # This prevents race condition where callback fires before database insert completes
        order_id = None
        if is_paper:
            # For paper trading, generate order_id ourselves before placing order
            import uuid
            order_id = str(uuid.uuid4())
            
            # Save order to database FIRST (before place_order() which may call callback synchronously)
            if self.trade_repo:
                try:
                    order_doc = {
                        "order_id": order_id,
                        "symbol": contract.symbol if hasattr(contract, 'symbol') else "N/A",
                        "contract": contract.symbol if hasattr(contract, 'symbol') else "N/A",
                        "quantity": quantity,
                        "order_type": str(self.order_type),
                        "signal": signal,
                        "price": limit_price if limit_price else 0.0,
                        "status": "PENDING",
                        "timestamp": datetime.utcnow(),
                        "paper_trading": True
                    }
                    self.trade_repo.orders.insert_one(order_doc)
                    self.logger.info(f"Saved order to database: {order_id} | Status: PENDING")
                except Exception as e:
                    self.logger.error(f"Failed to save order to database: {e}", exc_info=True)
                    # Continue anyway - order will be created in callback if needed
        
        # Prepare position entry structure BEFORE placing order
        # For MARKET orders, broker.place_order() may fill immediately and call callback synchronously
        position_entry = {
            "order_id": order_id,  # May be None for live trading, will be set after place_order returns
            "contract": contract,
            "quantity": quantity,
            "signal": signal,
            "entry_price": None,   # filled later (average price for partial fills)
            "filled_quantity": 0,
            "status": OrderStatus.PENDING,
            "order_ids": [order_id] if order_id else [],  # Track all order_ids for this position
        }

        # Place order with broker (pass order_id for paper trading to avoid race condition)
        returned_order_id = self.broker.place_order(
            contract=contract,
            variety=Variety.REGULAR,
            trade_action=TradeAction.BUY,
            quantity=quantity,
            disclosed_quantity=0,
            order_type=self.order_type,
            price=order_price,
            trigger_price=0.0,
            product_type=ProductType.MIS,
            time_in_force=TimeInForce.DAY,
            order_id=order_id if is_paper else None,  # Pass order_id for paper trading
        )
        # For live trading, use the order_id returned by broker
        if not is_paper:
            order_id = returned_order_id

        self.logger.info(f"Order placed: Order ID = {returned_order_id}")

        # Update position entry with actual order_id and add to open_positions
        position_entry["order_id"] = returned_order_id
        # If callback already fired and created an entry, update it; otherwise create new
        if returned_order_id in self.open_positions:
            # Update existing entry (callback may have created it)
            self.open_positions[returned_order_id].update(position_entry)
        else:
            # Create new entry
            self.open_positions[returned_order_id] = position_entry

        # Track pending order for timeout
        if self.order_type == OrderType.LIMIT:
            self.pending_orders[returned_order_id] = {
                "contract": contract,
                "quantity": quantity,
                "limit_price": limit_price,
                "placed_at": time.time(),
            }
            # Start timeout timer
            threading.Timer(
                self.order_timeout,
                self._check_and_cancel_order,
                args=(returned_order_id,)
            ).start()

        # For live trading, save order to database after getting order_id from broker
        if not is_paper and self.trade_repo:
            try:
                if hasattr(self.broker, 'get_order'):
                    order = self.broker.get_order(returned_order_id)
                    if order:
                        self.trade_repo.save_order(order)
                        self.logger.info(f"Saved order to database: {returned_order_id}")
            except Exception as e:
                self.logger.error(f"Failed to save order to database: {e}", exc_info=True)

    def _check_and_cancel_order(self, order_id: str):
        """Check if order should be cancelled due to timeout or price movement."""
        if order_id not in self.pending_orders:
            return  # Already filled or cancelled
        
        order_info = self.pending_orders[order_id]
        current_status = self.broker.get_order_status(order_id) if hasattr(self.broker, 'get_order_status') else OrderStatus.PENDING
        
        if current_status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
            # Order already processed - update database if needed
            if current_status == OrderStatus.CANCELLED and self.trade_repo:
                try:
                    # Ensure database reflects cancelled status
                    self.trade_repo.update_order(
                        order_id=order_id,
                        status="CANCELLED"
                    )
                except Exception as e:
                    self.logger.debug(f"Order {order_id} already cancelled, database update skipped or failed: {e}")
            
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            return
        
        # Check timeout: if order has been pending longer than timeout period
        placed_at = order_info.get("placed_at", 0)
        elapsed_time = time.time() - placed_at if placed_at > 0 else float('inf')
        
        if elapsed_time >= self.order_timeout:
            # Order timed out, cancel it
            self.logger.warning(f"Cancelling order {order_id}: Timeout after {elapsed_time:.1f}s (limit: {self.order_timeout}s)")
            if hasattr(self.broker, 'cancel_order'):
                try:
                    self.broker.cancel_order(order_id)
                except Exception as e:
                    self.logger.error(f"Failed to cancel order {order_id} via broker: {e}", exc_info=True)
            
            # Update database status to CANCELLED
            if self.trade_repo:
                try:
                    self.trade_repo.update_order(
                        order_id=order_id,
                        status="CANCELLED"
                    )
                    self.logger.info(f"Updated order {order_id} status to CANCELLED in database (timeout)")
                except Exception as e:
                    self.logger.error(f"Failed to update order status in database: {e}", exc_info=True)
            
            # Update position status
            if order_id in self.open_positions:
                self.open_positions[order_id]["status"] = OrderStatus.CANCELLED
                del self.open_positions[order_id]
            
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            return
        
        # Check if price moved beyond tolerance
        current_ltp = self.broker.get_ltp(order_info["contract"])
        if current_ltp > 0:
            price_change_pct = abs((current_ltp - order_info["limit_price"]) / order_info["limit_price"]) * 100
            
            if price_change_pct > self.price_tolerance_pct:
                # Price moved beyond tolerance, cancel order
                self.logger.warning(f"Cancelling order {order_id}: Price moved {price_change_pct:.2f}% beyond tolerance")
                if hasattr(self.broker, 'cancel_order'):
                    try:
                        self.broker.cancel_order(order_id)
                    except Exception as e:
                        self.logger.error(f"Failed to cancel order {order_id} via broker: {e}", exc_info=True)
                
                # Update database status to CANCELLED
                if self.trade_repo:
                    try:
                        self.trade_repo.update_order(
                            order_id=order_id,
                            status="CANCELLED"
                        )
                        self.logger.info(f"Updated order {order_id} status to CANCELLED in database (price tolerance exceeded)")
                    except Exception as e:
                        self.logger.error(f"Failed to update order status in database: {e}", exc_info=True)
                
                # Update position status
                if order_id in self.open_positions:
                    self.open_positions[order_id]["status"] = OrderStatus.CANCELLED
                    del self.open_positions[order_id]
                
                if order_id in self.pending_orders:
                    del self.pending_orders[order_id]

    # -------------------------------------------------
    # Broker event hooks (called from AA streamer)
    # -------------------------------------------------

    def on_order_filled(self, event):
        """
        Called when broker confirms order fill (full or partial).
        Handles partial fills and calculates average price.
        """
        order_id = event.order.order_id

        # If order not in open_positions yet, create it (can happen if callback fires before _place_entry_order completes)
        if order_id not in self.open_positions:
            # Try to get order details from broker
            signal = None
            if hasattr(self.broker, 'orders') and order_id in self.broker.orders:
                broker_order = self.broker.orders[order_id]
                signal = broker_order.get("signal")
            
            # Also try to get signal from database if order exists there
            if not signal and self.trade_repo:
                try:
                    existing_order = self.trade_repo.orders.find_one({"order_id": order_id})
                    if existing_order and existing_order.get("signal"):
                        signal = existing_order["signal"]
                except:
                    pass
            
            # Create position entry from broker order
            if hasattr(self.broker, 'orders') and order_id in self.broker.orders:
                broker_order = self.broker.orders[order_id]
                self.open_positions[order_id] = {
                    "order_id": order_id,
                    "contract": broker_order.get("contract"),
                    "quantity": broker_order.get("quantity", event.quantity),
                    "signal": signal,  # Try to get from broker order or database
                    "entry_price": None,
                    "filled_quantity": 0,
                    "status": OrderStatus.PENDING,
                    "order_ids": [order_id]  # Track all order_ids for this position
                }
                self.logger.debug(f"Created position entry for order {order_id} from broker order | Signal: {signal}")
            else:
                self.logger.warning(f"Order {order_id} filled but not found in open_positions or broker.orders - cannot process fill")
                # Still try to update database even if we can't process the position
                if self.trade_repo:
                    try:
                        is_paper = self.config.get("deployment", {}).get("paper_trading", False)
                        if is_paper:
                            status_str = "PARTIAL" if event.is_partial else "FILLED"
                            # Try to get signal from existing order in database
                            existing_order = self.trade_repo.orders.find_one({"order_id": order_id})
                            signal = existing_order.get("signal") if existing_order else None
                            
                            self.trade_repo.update_order(
                                order_id=order_id,
                                status=status_str,
                                filled_quantity=event.filled_quantity,
                                filled_price=event.filled_price,
                                signal=signal
                            )
                            self.logger.info(f"Updated order in database: {order_id} | Status: {status_str} | Price: {event.filled_price:.2f}")
                    except Exception as e:
                        self.logger.error(f"Failed to update order status in database: {e}", exc_info=True)
                return

        position = self.open_positions[order_id]
        
        # Get individual fills from broker if available (for partial fills tracking)
        individual_fills = []
        if hasattr(self.broker, 'order_fills') and order_id in self.broker.order_fills:
            individual_fills = self.broker.order_fills[order_id]
        
        # Create ENTRY trades and update aggregated DB position using *fill deltas* (idempotent)
        if self.trade_repo and position.get("contract") is not None:
            try:
                next_fill_number = int(position.get("_next_fill_number", 1) or 1)

                if individual_fills and len(individual_fills) > 0:
                    processed = int(position.get("_fills_processed", 0) or 0)
                    new_fills = individual_fills[processed:]

                    for fill_qty, fill_price, _ in new_fills:
                        # 1) Trade ledger
                        self.trade_repo.save_trade(
                            order_id=order_id,
                            trade_type="ENTRY",
                            price=fill_price,
                            quantity=fill_qty,
                            fill_number=next_fill_number
                        )
                        self.logger.info(
                            f"Created ENTRY trade: Order {order_id} | Fill #{next_fill_number} | Qty: {fill_qty} @ {fill_price:.2f}"
                        )

                        # 2) Aggregated position state
                        self.trade_repo.apply_entry_fill(
                            contract=position["contract"],
                            order_id=order_id,
                            quantity=int(fill_qty),
                            fill_price=float(fill_price),
                        )
                        next_fill_number += 1

                    position["_fills_processed"] = len(individual_fills)
                    position["_next_fill_number"] = next_fill_number
                else:
                    # Fallback for brokers that don't provide per-fill breakdown
                    accounted = int(position.get("_accounted_filled_quantity", 0) or 0)
                    delta = int(event.filled_quantity) - accounted
                    if delta > 0:
                        self.trade_repo.save_trade(
                            order_id=order_id,
                            trade_type="ENTRY",
                            price=event.filled_price,
                            quantity=delta,
                            fill_number=next_fill_number
                        )
                        self.logger.info(f"Created ENTRY trade: Order {order_id} | Qty: {delta} @ {event.filled_price:.2f}")

                        self.trade_repo.apply_entry_fill(
                            contract=position["contract"],
                            order_id=order_id,
                            quantity=int(delta),
                            fill_price=float(event.filled_price),
                        )
                        position["_accounted_filled_quantity"] = int(event.filled_quantity)
                        position["_next_fill_number"] = next_fill_number + 1
            except Exception as e:
                self.logger.error(f"Failed to save entry trade / update position: {e}", exc_info=True)
        
        # Update filled quantity
        if event.is_partial:
            # Partial fill: accumulate
            position["filled_quantity"] = event.filled_quantity
            position["status"] = OrderStatus.PARTIAL
            
            # Get average price from broker
            if hasattr(self.broker, 'get_average_fill_price'):
                avg_price = self.broker.get_average_fill_price(order_id)
                position["entry_price"] = avg_price
            else:
                position["entry_price"] = event.filled_price
            
            self.logger.info(f"Partial fill: {event.filled_quantity}/{position['quantity']} @ {event.filled_price:.2f}")
        else:
            # Full fill
            position["filled_quantity"] = event.filled_quantity
            position["entry_price"] = event.filled_price
            position["status"] = OrderStatus.FILLED
            
            # Remove from pending orders if LIMIT order
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            
            self.logger.info(f"Order filled: {event.filled_quantity} @ {event.filled_price:.2f}")

        # Ensure in-memory position quantity reflects *filled* quantity (critical for SL/TP sizing)
        if "order_quantity" not in position:
            position["order_quantity"] = position.get("quantity", 0)
        try:
            position["quantity"] = int(position.get("filled_quantity", position.get("quantity", 0)) or 0)
        except Exception:
            pass

        # Update order status in database when filled (ALWAYS update, even if position processing failed)
        if self.trade_repo:
            try:
                is_paper = self.config.get("deployment", {}).get("paper_trading", False)
                if is_paper:
                    # Update order status in database
                    status_str = "PARTIAL" if event.is_partial else "FILLED"
                    # Try to update, if not found, insert (upsert)
                    result = self.trade_repo.update_order(
                        order_id=order_id,
                        status=status_str,
                        filled_quantity=event.filled_quantity,
                        filled_price=event.filled_price,
                        upsert=False  # Don't create if doesn't exist, just update
                    )
                    if result and result.matched_count > 0:
                        self.logger.info(f"Updated order in database: {order_id} | Status: {status_str} | Price: {event.filled_price:.2f}")
                    else:
                        # Order not found - this should be rare now since we save before placing order
                        # But handle it gracefully as fallback
                        self.logger.warning(f"Order {order_id} not found in database, creating it now (fallback)")
                        contract = position.get("contract")
                        
                        # Try to get signal from position, or from existing order in database, or from broker order
                        signal = position.get("signal")
                        if not signal:
                            # Try to get from existing order if it exists
                            existing_order = self.trade_repo.orders.find_one({"order_id": order_id})
                            if existing_order and existing_order.get("signal"):
                                signal = existing_order["signal"]
                            # If still None, try to get from broker order
                            if not signal and hasattr(self.broker, 'orders') and order_id in self.broker.orders:
                                broker_order = self.broker.orders[order_id]
                                signal = broker_order.get("signal")
                        
                        order_doc = {
                            "order_id": order_id,
                            "symbol": contract.symbol if contract and hasattr(contract, 'symbol') else "N/A",
                            "contract": contract.symbol if contract and hasattr(contract, 'symbol') else "N/A",
                            "quantity": position.get("quantity", event.filled_quantity),
                            "order_type": "MKT",
                            "signal": signal if signal else None,  # Keep as None if not found, don't use "N/A"
                            "price": 0.0,
                            "status": status_str,
                            "filled_quantity": event.filled_quantity,
                            "filled_price": event.filled_price,
                            "timestamp": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                            "paper_trading": True
                        }
                        self.trade_repo.orders.insert_one(order_doc)
                        self.logger.info(f"Created order in database: {order_id} | Status: {status_str} | Price: {event.filled_price:.2f} | Signal: {signal}")
            except Exception as e:
                self.logger.error(f"Failed to update order status in database: {e}", exc_info=True)

        # Register position with exit manager (partial fills are always allowed)
        # Position persistence is handled above via apply_entry_fill (fill-delta safe).

        # Notify risk manager
        self.risk_manager.on_new_position(
            order_id=order_id,
            position=position
        )

    def on_order_exit(self, order_id: str):
        """
        Called when position is exited (SL/TP/square-off).
        """

        if order_id in self.open_positions:
            del self.open_positions[order_id]
