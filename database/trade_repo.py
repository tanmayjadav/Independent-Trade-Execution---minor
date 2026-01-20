from datetime import datetime, date
from pymongo.errors import PyMongoError
from database.mongo_client import MongoDBClient
from database.model import (
    order_to_doc,
    trade_to_doc,
    position_to_doc,
    daily_summary_to_doc,
)
import uuid


class TradeRepository:

    def __init__(self, mongo_uri: str, db_name="ema_xts"):
        client = MongoDBClient.get_client(mongo_uri)
        self.db = client[db_name]

        self.orders = self.db.orders
        self.trades = self.db.trades
        self.positions = self.db.positions
        self.daily_summary = self.db.daily_summary

    # ----------------------------
    # Orders
    # ----------------------------
    def save_order(self, order):
        try:
            self.orders.insert_one(order_to_doc(order))
        except PyMongoError:
            pass

    def update_order(
        self,
        order_id: str,
        status: str = None,
        filled_quantity: int = None,
        filled_price: float = None,
        signal: str = None,
        upsert: bool = False,
    ):
        """
        Update an existing order in the database.
        
        Args:
            order_id: The order ID to update
            status: Order status (e.g., "FILLED", "PARTIAL", "CANCELLED")
            filled_quantity: Quantity that was filled
            filled_price: Price at which order was filled
            signal: Trading signal (optional, only set if not already present)
            upsert: If True, create order if it doesn't exist (default: False)
        
        Returns:
            UpdateResult from MongoDB
        """
        try:
            update_doc = {"updated_at": datetime.utcnow()}
            
            if status is not None:
                update_doc["status"] = status
            if filled_quantity is not None:
                update_doc["filled_quantity"] = filled_quantity
            if filled_price is not None:
                update_doc["filled_price"] = filled_price
            
            # Only set signal if provided and order doesn't already have it
            if signal:  # Only if signal is truthy (not None/empty)
                existing_order = self.orders.find_one({"order_id": order_id})
                if existing_order and not existing_order.get("signal"):
                    update_doc["signal"] = signal
                elif not existing_order:
                    # Order doesn't exist yet, set signal
                    update_doc["signal"] = signal
            
            result = self.orders.update_one(
                {"order_id": order_id},
                {"$set": update_doc},
                upsert=upsert
            )
            return result
        except PyMongoError:
            return None

    # ----------------------------
    # Trades
    # ----------------------------
    def save_trade(
        self,
        order_id: str,
        trade_type: str,  # "ENTRY" or "EXIT"
        price: float,  # Fill price
        quantity: int,
        pnl: float = 0.0,  # Only for EXIT trades
        reason: str = None,  # Only for EXIT trades
        entry_price: float = None,  # For EXIT trades
        exit_price: float = None,  # For EXIT trades
        fill_number: int = 1,  # For partial fills
        symbol: str = None,  # Optional: for analyzer-style exports
        entry_order_id: str = None,  # Optional: link EXIT back to entry order
        entry_datetime: datetime = None,  # Optional: entry timestamp
    ):
        """
        Save a trade to database.
        
        For ENTRY trades: save_trade(order_id, "ENTRY", fill_price, quantity, fill_number=1)
        For EXIT trades: save_trade(order_id, "EXIT", exit_price, quantity, pnl, reason, entry_price, exit_price)
        """
        try:
            self.trades.insert_one(
                trade_to_doc(
                    order_id=order_id,
                    trade_type=trade_type,
                    price=price,
                    quantity=quantity,
                    pnl=pnl,
                    reason=reason,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    fill_number=fill_number,
                    symbol=symbol,
                    entry_order_id=entry_order_id,
                    entry_datetime=entry_datetime,
                )
            )
        except PyMongoError:
            pass

    # ----------------------------
    # Positions
    # ----------------------------
    def _compute_pnls(self, entry_price: float, last_price: float, open_qty: int, realized_pnl: float):
        unrealized = 0.0
        if entry_price is not None and last_price is not None and open_qty is not None:
            try:
                unrealized = (float(last_price) - float(entry_price)) * int(open_qty)
            except Exception:
                unrealized = 0.0
        net = float(realized_pnl or 0.0) + float(unrealized or 0.0)
        return float(unrealized), float(net)

    def apply_entry_fill(self, contract, order_id: str, quantity: int, fill_price: float):
        """
        Apply an ENTRY fill to the aggregated OPEN position for a symbol.

        - Increases open quantity
        - Updates weighted-average entry price (average cost)
        - Preserves created_at for the open position
        """
        if quantity is None or quantity <= 0:
            return

        try:
            symbol = contract.symbol
            now = datetime.utcnow()

            existing = self.positions.find_one({"symbol": symbol, "status": "OPEN"})
            if not existing:
                # Create new OPEN position
                doc = {
                    "position_id": str(uuid.uuid4()),
                    "contract": contract,  # not stored by position_to_doc; kept here only for callers that pass dicts
                    "quantity": int(quantity),
                    "opened_quantity": int(quantity),
                    "closed_quantity": 0,
                    "entry_price": float(fill_price),
                    "exit_price": None,
                    "last_price": float(fill_price),
                    "realized_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "net_pnl": 0.0,
                    "order_ids": [order_id] if order_id else [],
                    "exit_order_ids": [],
                    "created_at": now,
                    "updated_at": now,
                    "closed_at": None,
                }
                # position_to_doc expects a dict with contract; we pass it through to preserve compatibility
                self.positions.insert_one(position_to_doc(doc, status="OPEN"))
                return

            # Aggregate into existing OPEN position
            existing_qty = int(existing.get("quantity", 0) or 0)
            existing_entry = existing.get("entry_price")
            existing_entry = float(existing_entry) if existing_entry is not None else 0.0

            new_qty = int(quantity)
            new_price = float(fill_price)

            total_qty = existing_qty + new_qty
            if total_qty > 0:
                weighted_entry = ((existing_entry * existing_qty) + (new_price * new_qty)) / total_qty
            else:
                weighted_entry = new_price

            order_ids = existing.get("order_ids", []) or []
            if order_id and order_id not in order_ids:
                order_ids.append(order_id)

            opened_qty = int(existing.get("opened_quantity", existing_qty) or 0) + new_qty
            realized = float(existing.get("realized_pnl", 0.0) or 0.0)
            last_price = float(existing.get("last_price", new_price) or new_price)
            unrealized, net = self._compute_pnls(weighted_entry, last_price, total_qty, realized)

            self.positions.update_one(
                {"_id": existing["_id"]},
                {"$set": {
                    "entry_price": float(weighted_entry),
                    "quantity": int(total_qty),
                    "opened_quantity": int(opened_qty),
                    "order_ids": order_ids,
                    "last_price": last_price,
                    "unrealized_pnl": float(unrealized),
                    "net_pnl": float(net),
                    "updated_at": now,
                }}
            )
        except PyMongoError:
            pass
        except Exception:
            pass

    def apply_exit_fill(self, contract, exit_order_id: str, quantity: int, exit_price: float, reason: str = None):
        """
        Apply an EXIT fill to the aggregated OPEN position for a symbol.

        - Decreases open quantity (supports partial exits)
        - Accumulates realized_pnl
        - Tracks weighted-average exit_price across closed_quantity
        - Marks CLOSED when quantity reaches 0 (sets closed_at)
        """
        if quantity is None or quantity <= 0:
            return

        try:
            symbol = contract.symbol
            now = datetime.utcnow()

            existing = self.positions.find_one({"symbol": symbol, "status": "OPEN"})
            if not existing:
                return

            open_qty = int(existing.get("quantity", 0) or 0)
            if open_qty <= 0:
                return

            exit_qty = min(int(quantity), open_qty)
            entry_price = existing.get("entry_price")
            entry_price = float(entry_price) if entry_price is not None else float(exit_price)
            px = float(exit_price)

            realized_prev = float(existing.get("realized_pnl", 0.0) or 0.0)
            realized_inc = (px - entry_price) * exit_qty
            realized_new = realized_prev + realized_inc

            closed_qty_prev = int(existing.get("closed_quantity", 0) or 0)
            closed_qty_new = closed_qty_prev + exit_qty

            # Weighted average exit price across all closed quantity
            prev_exit_px = existing.get("exit_price")
            prev_exit_px = float(prev_exit_px) if prev_exit_px is not None else 0.0
            if closed_qty_new > 0:
                weighted_exit = ((prev_exit_px * closed_qty_prev) + (px * exit_qty)) / closed_qty_new
            else:
                weighted_exit = px

            remaining_qty = open_qty - exit_qty

            exit_order_ids = existing.get("exit_order_ids", []) or []
            if exit_order_id and exit_order_id not in exit_order_ids:
                exit_order_ids.append(exit_order_id)

            last_price = px
            unrealized, net = self._compute_pnls(entry_price, last_price, remaining_qty, realized_new)

            update_doc = {
                "quantity": int(remaining_qty),
                "closed_quantity": int(closed_qty_new),
                "exit_price": float(weighted_exit),
                "exit_order_ids": exit_order_ids,
                "last_price": last_price,
                "realized_pnl": float(realized_new),
                "unrealized_pnl": float(unrealized),
                "net_pnl": float(net),
                "updated_at": now,
            }

            if remaining_qty == 0:
                update_doc["status"] = "CLOSED"
                update_doc["closed_at"] = now

            self.positions.update_one({"_id": existing["_id"]}, {"$set": update_doc})
        except PyMongoError:
            pass
        except Exception:
            pass

    def update_mark_to_market(self, contract, ltp: float):
        """
        Update last_price and unrealized/net PnL for the OPEN position for a symbol.
        This is safe to call frequently (writes one small $set).
        """
        try:
            symbol = contract.symbol
            existing = self.positions.find_one({"symbol": symbol, "status": "OPEN"})
            if not existing:
                return

            entry_price = existing.get("entry_price")
            if entry_price is None:
                return

            qty = int(existing.get("quantity", 0) or 0)
            realized = float(existing.get("realized_pnl", 0.0) or 0.0)
            last_price = float(ltp)
            unrealized, net = self._compute_pnls(float(entry_price), last_price, qty, realized)

            self.positions.update_one(
                {"_id": existing["_id"]},
                {"$set": {
                    "last_price": last_price,
                    "unrealized_pnl": float(unrealized),
                    "net_pnl": float(net),
                    "updated_at": datetime.utcnow(),
                }}
            )
        except PyMongoError:
            pass
        except Exception:
            pass

    def upsert_position(self, position, status):
        """
        Backward-compatible API.

        - status="OPEN": treated as an ENTRY addition (uses position.quantity + position.entry_price)
        - status="CLOSED": treated as a full EXIT for that symbol (uses position.quantity + position.exit_price)
        """
        try:
            contract = position["contract"]
            if status == "OPEN":
                self.apply_entry_fill(
                    contract=contract,
                    order_id=position.get("order_id"),
                    quantity=int(position.get("quantity", 0) or 0),
                    fill_price=float(position.get("entry_price") or 0.0),
                )
            elif status == "CLOSED":
                self.apply_exit_fill(
                    contract=contract,
                    exit_order_id=position.get("order_id"),
                    quantity=int(position.get("quantity", 0) or 0),
                    exit_price=float(position.get("exit_price") or position.get("price") or 0.0),
                    reason=None,
                )
        except PyMongoError:
            pass

    # ----------------------------
    # Daily Summary
    # ----------------------------
    def save_daily_summary(
        self,
        date,
        total_trades,
        wins,
        losses,
        net_pnl,
        max_drawdown,
    ):
        try:
            self.daily_summary.insert_one(
                daily_summary_to_doc(
                    date,
                    total_trades,
                    wins,
                    losses,
                    net_pnl,
                    max_drawdown,
                )
            )
        except PyMongoError:
            pass

    def get_daily_summary(self, target_date: date = None):
        """
        Get daily summary for a specific date (defaults to today).
        Returns None if no summary exists for that date.
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        # Convert date to datetime at midnight for query (match how we store it)
        if isinstance(target_date, date) and not isinstance(target_date, datetime):
            target_datetime = datetime.combine(target_date, datetime.min.time())
        elif isinstance(target_date, datetime):
            target_datetime = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_datetime = target_date
        
        try:
            # Query for the exact datetime (stored as midnight)
            summary = self.daily_summary.find_one({"date": target_datetime})
            return summary
        except PyMongoError:
            return None

    def get_today_trades(self):
        """
        Get all trades executed today.
        Returns list of trade documents.
        """
        return self.get_date_trades(datetime.now().date())

    def get_date_trades(self, target_date: date):
        """
        Get all trades executed on a specific date.
        Returns list of trade documents.
        """
        try:
            date_start = datetime.combine(target_date, datetime.min.time())
            date_end = datetime.combine(target_date, datetime.max.time())
            
            trades = list(self.trades.find({
                "timestamp": {
                    "$gte": date_start,
                    "$lte": date_end
                }
            }).sort("timestamp", 1))
            
            return trades
        except PyMongoError:
            return []

    def get_today_orders(self):
        """
        Get all orders placed today.
        Returns list of order documents.
        """
        return self.get_date_orders(datetime.now().date())

    def get_date_orders(self, target_date: date):
        """
        Get all orders placed on a specific date.
        Returns list of order documents.
        """
        try:
            date_start = datetime.combine(target_date, datetime.min.time())
            date_end = datetime.combine(target_date, datetime.max.time())
            
            orders = list(self.orders.find({
                "timestamp": {
                    "$gte": date_start,
                    "$lte": date_end
                }
            }).sort("timestamp", 1))
            
            return orders
        except PyMongoError:
            return []

    def get_today_positions(self):
        """
        Get all positions updated today.
        Returns list of position documents.
        """
        return self.get_date_positions(datetime.now().date())

    def get_date_positions(self, target_date: date):
        """
        Get all positions updated on a specific date.
        Returns list of position documents.
        """
        try:
            date_start = datetime.combine(target_date, datetime.min.time())
            date_end = datetime.combine(target_date, datetime.max.time())
            
            positions = list(self.positions.find({
                "updated_at": {
                    "$gte": date_start,
                    "$lte": date_end
                }
            }).sort("updated_at", 1))
            
            return positions
        except PyMongoError:
            return []

    def get_today_stats(self):
        """
        Calculate comprehensive statistics for today's trading session.
        Returns a dictionary with all metrics.
        """
        return self.get_date_stats(datetime.now().date())

    def get_date_stats(self, target_date: date):
        """
        Calculate comprehensive statistics for a specific date's trading session.
        Returns a dictionary with all metrics.
        """
        date_trades = self.get_date_trades(target_date)
        
        if not date_trades:
            return {
                "date": target_date.isoformat(),
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "net_pnl": 0.0,
                "win_rate": 0.0,
                "win_loss_ratio": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "max_win": 0.0,
                "max_loss": 0.0,
                "max_drawdown": 0.0,
            }
        
        wins = []
        losses = []
        net_pnl = 0.0
        equity_curve = [0.0]
        max_drawdown = 0.0
        
        for trade in date_trades:
            pnl = trade.get("pnl", 0.0)
            net_pnl += pnl
            
            if pnl > 0:
                wins.append(pnl)
            elif pnl < 0:
                losses.append(abs(pnl))
            
            # Update equity curve for drawdown calculation
            current_equity = equity_curve[-1] + pnl
            equity_curve.append(current_equity)
            
            # Calculate drawdown
            peak = max(equity_curve)
            drawdown = peak - current_equity
            max_drawdown = max(max_drawdown, drawdown)
        
        total_trades = len(date_trades)
        win_count = len(wins)
        loss_count = len(losses)
        
        # Calculate metrics
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
        win_loss_ratio = (win_count / loss_count) if loss_count > 0 else (float('inf') if win_count > 0 else 0.0)
        avg_win = (sum(wins) / win_count) if win_count > 0 else 0.0
        avg_loss = (sum(losses) / loss_count) if loss_count > 0 else 0.0
        
        # Profit factor = Total wins / Total losses
        total_wins = sum(wins)
        total_losses = sum(losses)
        profit_factor = (total_wins / total_losses) if total_losses > 0 else (float('inf') if total_wins > 0 else 0.0)
        
        max_win = max(wins) if wins else 0.0
        max_loss = max(losses) if losses else 0.0
        
        return {
            "date": target_date.isoformat(),
            "total_trades": total_trades,
            "wins": win_count,
            "losses": loss_count,
            "net_pnl": round(net_pnl, 2),
            "win_rate": round(win_rate, 2),
            "win_loss_ratio": round(win_loss_ratio, 2) if win_loss_ratio != float('inf') else "∞",
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else "∞",
            "max_win": round(max_win, 2),
            "max_loss": round(max_loss, 2),
            "max_drawdown": round(max_drawdown, 2),
        }
