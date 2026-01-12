# broker/paper_broker.py

from variance_connect.utils.enums import TradeAction, OrderType
from execution.trade_controller import OrderStatus
import uuid
import time
import threading


class OrderFilledEvent:
    """Mock event object for paper trading order fills."""
    def __init__(self, order_id, contract, filled_price, quantity, filled_quantity=None, is_partial=False):
        self.order = type('Order', (), {'order_id': order_id})()
        self.contract = contract
        self.filled_price = filled_price  # Average price for partial fills
        self.quantity = quantity  # Total order quantity
        self.filled_quantity = filled_quantity or quantity  # Quantity filled in this event
        self.is_partial = is_partial


class PaperBroker:
    """
    Simulated broker for paper trading.
    Interface-compatible with variance_connect broker usage.
    """

    def __init__(self, starting_capital: float = 1_000_000):
        self.balance = starting_capital

        self.orders = {}       # order_id -> order dict
        self.positions = {}    # order_id -> position dict
        self.ltp_cache = {}    # token -> price
        self.order_filled_callback = None  # Callback for order filled events
        
        # Partial fill tracking
        self.order_fills = {}  # order_id -> list of (quantity, price, timestamp)
        self.order_status = {}  # order_id -> OrderStatus
        self.pending_orders = {}  # order_id -> order details for limit orders
        self.stop_orders = {}  # order_id -> stop order details (for STOP orders)

    # -------------------------------------------------
    # Market data hook
    # -------------------------------------------------

    def on_tick(self, event):
        """
        Update LTP cache and check stop orders and limit orders.
        Also fill pending MARKET orders that were waiting for LTP.
        """
        token = event.contract.token
        ltp = float(event.ltp)
        symbol = event.contract.symbol if hasattr(event.contract, 'symbol') else 'N/A'
        
        # Update LTP cache
        self.ltp_cache[token] = ltp
        
        # Check if any pending MARKET orders are waiting for LTP for this contract
        for order_id, pending_order in list(self.pending_orders.items()):
            if pending_order.get("waiting_for_ltp") and pending_order["contract"].token == token:
                # LTP is now available - fill the order
                if ltp > 0:
                    order = self.orders.get(order_id)
                    if order and order["order_type"] == OrderType.MARKET:
                        # Remove from pending and fill it
                        del self.pending_orders[order_id]
                        self._process_market_order(
                            order_id,
                            pending_order["contract"],
                            pending_order["trade_action"],
                            pending_order["quantity"],
                            ltp
                        )
        
        # Check LIMIT orders (take-profit orders) - fill when price is favorable
        for order_id, pending_order in list(self.pending_orders.items()):
            if pending_order.get("waiting_for_ltp"):
                continue  # Skip MARKET orders waiting for LTP
            
            if pending_order["contract"].token != token:
                continue
            
            limit_price = pending_order.get("limit_price")
            if limit_price is None:
                continue
            
            trade_action = pending_order["trade_action"]
            
            # Check if price is favorable for limit order
            if trade_action == TradeAction.SELL:
                # SELL limit (TP): Fill if LTP >= limit_price
                if ltp >= limit_price:
                    self._process_limit_order_fill(order_id, pending_order["contract"], trade_action, 
                                                  pending_order["quantity"], ltp, limit_price)
            elif trade_action == TradeAction.BUY:
                # BUY limit: Fill if LTP <= limit_price
                if ltp <= limit_price:
                    self._process_limit_order_fill(order_id, pending_order["contract"], trade_action,
                                                  pending_order["quantity"], ltp, limit_price)
        
        # Check STOP orders (stop-loss orders)
        for order_id, stop_order in list(self.stop_orders.items()):
            if stop_order["contract"].token != token:
                continue
            
            if stop_order["status"] != "PENDING":
                continue
            
            # Check if trigger price is hit
            trigger_price = stop_order["trigger_price"]
            trade_action = stop_order["trade_action"]
            
            # For SELL stop-loss: Trigger when price drops to or below trigger
            if trade_action == TradeAction.SELL and ltp <= trigger_price:
                # Execute stop order as MARKET order
                self._execute_stop_order(order_id, stop_order, ltp)
            # For BUY stop-loss: Trigger when price rises to or above trigger
            elif trade_action == TradeAction.BUY and ltp >= trigger_price:
                # Execute stop order as MARKET order
                self._execute_stop_order(order_id, stop_order, ltp)

    def get_ltp(self, contract):
        return self.ltp_cache.get(contract.token, 0.0)

    # -------------------------------------------------
    # Account
    # -------------------------------------------------

    def get_account_balance(self):
        return self.balance

    def set_order_filled_callback(self, callback):
        """Set callback to be called when orders are filled."""
        self.order_filled_callback = callback

    # -------------------------------------------------
    # Orders
    # -------------------------------------------------

    def place_order(
        self,
        contract,
        variety,
        trade_action,
        quantity,
        disclosed_quantity,
        order_type,
        price,
        trigger_price,
        product_type,
        time_in_force,
        order_id=None,
    ):
        """
        Places order (MARKET or LIMIT).
        For MARKET: Fills immediately (with possible partial fills)
        For LIMIT: Queues order and fills when price is favorable
        
        Args:
            order_id: Optional order_id to use (for paper trading to avoid race conditions)
        """
        ltp = self.get_ltp(contract)
        symbol = contract.symbol if hasattr(contract, 'symbol') else 'N/A'
        
        # Use provided order_id or generate new one
        if order_id is None:
            order_id = str(uuid.uuid4())
        
        # For paper trading, if LTP is not available, handle gracefully
        if ltp <= 0:
            if order_type == OrderType.LIMIT and price > 0:
                # Use limit price as fallback for LIMIT orders
                ltp = price
            elif order_type == OrderType.MARKET:
                # For MARKET orders, store order to fill when LTP arrives via on_tick
                order = {
                    "order_id": order_id,
                    "contract": contract,
                    "quantity": quantity,
                    "side": trade_action,
                    "order_type": order_type,
                    "limit_price": None,
                    "status": OrderStatus.PENDING,
                    "created_at": time.time(),
                }
                self.orders[order_id] = order
                self.order_status[order_id] = OrderStatus.PENDING
                self.order_fills[order_id] = []
                # Store in pending_orders to fill when LTP arrives
                self.pending_orders[order_id] = {
                    "order": order,
                    "contract": contract,
                    "trade_action": trade_action,
                    "quantity": quantity,
                    "limit_price": None,
                    "waiting_for_ltp": True
                }
                return order_id

        order = {
            "order_id": order_id,
            "contract": contract,
            "quantity": quantity,
            "side": trade_action,
            "order_type": order_type,
            "limit_price": price if order_type == OrderType.LIMIT else None,
            "status": OrderStatus.PENDING,
            "created_at": time.time(),
        }

        self.orders[order_id] = order
        self.order_status[order_id] = OrderStatus.PENDING
        self.order_fills[order_id] = []

        if order_type == OrderType.MARKET:
            # MARKET order: Fill immediately (with possible partial fills)
            self._process_market_order(order_id, contract, trade_action, quantity, ltp)
        elif order_type == OrderType.LIMIT:
            # LIMIT order: Store and process when price is favorable
            self.pending_orders[order_id] = {
                "order": order,
                "contract": contract,
                "trade_action": trade_action,
                "quantity": quantity,
                "limit_price": price,
            }
            # Start monitoring limit order
            self._monitor_limit_order(order_id, contract, trade_action, quantity, price, ltp)
        elif order_type == OrderType.STOP:
            # STOP order: Store and monitor trigger price
            self.stop_orders[order_id] = {
                "order": order,
                "contract": contract,
                "trade_action": trade_action,
                "quantity": quantity,
                "trigger_price": trigger_price,
                "status": "PENDING",
            }
            self.order_status[order_id] = OrderStatus.PENDING

        return order_id

    def _process_market_order(self, order_id, contract, trade_action, quantity, current_price):
        """Process MARKET order with possible partial fills."""
        if trade_action == TradeAction.BUY:
            # Simulate partial fills for MARKET orders
            # For paper trading, we'll simulate 1-3 partial fills
            import random
            num_fills = random.randint(1, 3) if quantity >= 50 else 1
            
            total_filled = 0
            total_cost = 0.0
            fills = []

            for i in range(num_fills):
                if total_filled >= quantity:
                    break
                
                # Simulate price variation (±1%)
                price_variation = random.uniform(-0.01, 0.01)
                fill_price = current_price * (1 + price_variation)
                
                # Calculate fill quantity
                if i == num_fills - 1:
                    # Last fill: remaining quantity
                    fill_qty = quantity - total_filled
                else:
                    # Partial fill: 30-70% of remaining
                    remaining = quantity - total_filled
                    fill_qty = int(remaining * random.uniform(0.3, 0.7))
                
                fill_cost = fill_price * fill_qty
                
                # Check balance
                if fill_cost > self.balance:
                    # Can't fill more
                    break
                
                self.balance -= fill_cost
                total_filled += fill_qty
                total_cost += fill_cost
                
                fills.append((fill_qty, fill_price, time.time()))
                self.order_fills[order_id].append((fill_qty, fill_price, time.time()))

            if total_filled == 0:
                self.order_status[order_id] = OrderStatus.REJECTED
                return

            # Calculate average price
            avg_price = total_cost / total_filled if total_filled > 0 else current_price

            # Update order status
            if total_filled < quantity:
                self.order_status[order_id] = OrderStatus.PARTIAL
            else:
                self.order_status[order_id] = OrderStatus.FILLED

            # Create position
            self.positions[order_id] = {
                "contract": contract,
                "quantity": total_filled,
                "entry_price": avg_price,
            }

            # Trigger callback for each fill
            if self.order_filled_callback:
                is_partial = total_filled < quantity
                event = OrderFilledEvent(
                    order_id=order_id,
                    contract=contract,
                    filled_price=avg_price,
                    quantity=quantity,
                    filled_quantity=total_filled,
                    is_partial=is_partial
                )
                try:
                    # Call callback directly (not in thread) to ensure it executes and errors are visible
                    self.order_filled_callback(event)
                except Exception as e:
                    import traceback
                    traceback.print_exc()

        elif trade_action == TradeAction.SELL:
            # Find matching position and exit
            for oid, pos in list(self.positions.items()):
                if pos["contract"].token == contract.token:
                    pnl = (current_price - pos["entry_price"]) * pos["quantity"]
                    self.balance += (current_price * pos["quantity"]) + pnl
                    self.order_status[order_id] = OrderStatus.FILLED
                    del self.positions[oid]
                    break

    def _monitor_limit_order(self, order_id, contract, trade_action, quantity, limit_price, current_price):
        """Monitor limit order and fill when price is favorable."""
        def check_and_fill():
            max_checks = 30  # Check for 30 seconds (1 check per second)
            checks = 0
            
            while checks < max_checks:
                if order_id not in self.pending_orders:
                    return  # Order cancelled or filled
                
                ltp = self.get_ltp(contract)
                if ltp <= 0:
                    time.sleep(1)
                    checks += 1
                    continue
                
                # Check if price is favorable
                if trade_action == TradeAction.BUY:
                    # BUY limit: Fill if LTP <= limit_price
                    if ltp <= limit_price:
                        self._process_limit_order_fill(order_id, contract, trade_action, quantity, ltp, limit_price)
                        return
                elif trade_action == TradeAction.SELL:
                    # SELL limit: Fill if LTP >= limit_price
                    if ltp >= limit_price:
                        self._process_limit_order_fill(order_id, contract, trade_action, quantity, ltp, limit_price)
                        return
                
                time.sleep(1)
                checks += 1
            
            # Timeout: Cancel order
            if order_id in self.pending_orders:
                self.cancel_order(order_id)
        
        threading.Thread(target=check_and_fill, daemon=True).start()

    def _process_limit_order_fill(self, order_id, contract, trade_action, quantity, fill_price, limit_price):
        """Process limit order fill."""
        if order_id not in self.pending_orders:
            return
        
        del self.pending_orders[order_id]
        self.order_status[order_id] = OrderStatus.FILLED
        
        if trade_action == TradeAction.BUY:
            cost = fill_price * quantity
            if cost > self.balance:
                self.order_status[order_id] = OrderStatus.REJECTED
                return
            
            self.balance -= cost
            
            self.positions[order_id] = {
                "contract": contract,
                "quantity": quantity,
                "entry_price": fill_price,
            }
            
            self.order_fills[order_id].append((quantity, fill_price, time.time()))
        elif trade_action == TradeAction.SELL:
            # SELL limit order (TP order): Find matching position and exit
            for oid, pos in list(self.positions.items()):
                if pos["contract"].token == contract.token:
                    pnl = (fill_price - pos["entry_price"]) * pos["quantity"]
                    self.balance += (fill_price * pos["quantity"]) + pnl
                    del self.positions[oid]
                    break
            
            self.order_fills[order_id].append((quantity, fill_price, time.time()))
        
        # Trigger callback
        if self.order_filled_callback:
            event = OrderFilledEvent(
                order_id=order_id,
                contract=contract,
                filled_price=fill_price,
                quantity=quantity,
                filled_quantity=quantity,
                is_partial=False
            )
            try:
                # Call callback directly (not in thread) to ensure it executes and errors are visible
                self.order_filled_callback(event)
            except Exception as e:
                import traceback
                traceback.print_exc()

    def cancel_order(self, order_id: str):
        """Cancel a pending order (limit or stop)."""
        if order_id in self.pending_orders:
            del self.pending_orders[order_id]
            self.order_status[order_id] = OrderStatus.CANCELLED
            if order_id in self.orders:
                self.orders[order_id]["status"] = OrderStatus.CANCELLED
            return True
        elif order_id in self.stop_orders:
            self.stop_orders[order_id]["status"] = "CANCELLED"
            self.order_status[order_id] = OrderStatus.CANCELLED
            if order_id in self.orders:
                self.orders[order_id]["status"] = OrderStatus.CANCELLED
            return True
        return False
    
    def _execute_stop_order(self, order_id: str, stop_order: dict, current_price: float):
        """Execute stop order when trigger price is hit."""
        if stop_order["status"] != "PENDING":
            return
        
        stop_order["status"] = "TRIGGERED"
        self.order_status[order_id] = OrderStatus.FILLED
        
        contract = stop_order["contract"]
        trade_action = stop_order["trade_action"]
        quantity = stop_order["quantity"]
        
        # Execute as MARKET order
        if trade_action == TradeAction.SELL:
            # Find matching position and exit
            for oid, pos in list(self.positions.items()):
                if pos["contract"].token == contract.token:
                    pnl = (current_price - pos["entry_price"]) * pos["quantity"]
                    self.balance += (current_price * pos["quantity"]) + pnl
                    del self.positions[oid]
                    break
        elif trade_action == TradeAction.BUY:
            # For buy stop (not used in our strategy, but handle it)
            cost = current_price * quantity
            if cost <= self.balance:
                self.balance -= cost
                self.positions[order_id] = {
                    "contract": contract,
                    "quantity": quantity,
                    "entry_price": current_price,
                }
        
        # Trigger callback if set
        if self.order_filled_callback:
            event = OrderFilledEvent(
                order_id=order_id,
                contract=contract,
                filled_price=current_price,
                quantity=quantity,
                filled_quantity=quantity,
                is_partial=False
            )
            import threading
            threading.Thread(target=self.order_filled_callback, args=(event,), daemon=True).start()

    def get_order_status(self, order_id: str):
        """Get current status of an order."""
        return self.order_status.get(order_id, OrderStatus.PENDING)

    def get_average_fill_price(self, order_id: str) -> float:
        """Calculate average fill price from all fills."""
        if order_id not in self.order_fills or not self.order_fills[order_id]:
            return 0.0
        
        fills = self.order_fills[order_id]
        total_qty = sum(f[0] for f in fills)
        total_cost = sum(f[0] * f[1] for f in fills)
        
        return total_cost / total_qty if total_qty > 0 else 0.0

    def get_filled_quantity(self, order_id: str) -> int:
        """Get total filled quantity for an order."""
        if order_id not in self.order_fills:
            return 0
        return sum(f[0] for f in self.order_fills[order_id])
