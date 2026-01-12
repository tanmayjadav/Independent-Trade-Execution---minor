from datetime import datetime, date
from pymongo.errors import PyMongoError
from database.mongo_client import MongoDBClient
from database.model import (
    order_to_doc,
    trade_to_doc,
    position_to_doc,
    daily_summary_to_doc,
)


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
                )
            )
        except PyMongoError:
            pass

    # ----------------------------
    # Positions
    # ----------------------------
    def upsert_position(self, position, status):
        """
        Upsert position by symbol.
        For multiple orders on same symbol, aggregates quantity and calculates weighted average price.
        """
        try:
            symbol = position["contract"].symbol
            
            # Get existing position if it exists
            existing_pos = self.positions.find_one({"symbol": symbol, "status": "OPEN"})
            
            if existing_pos and status == "OPEN":
                # Aggregate with existing position: calculate weighted average price
                existing_qty = existing_pos.get("quantity", 0)
                existing_price = existing_pos.get("entry_price", 0)
                new_qty = position.get("quantity", 0)
                new_price = position.get("entry_price", 0)
                
                # Calculate weighted average
                total_qty = existing_qty + new_qty
                if total_qty > 0:
                    weighted_avg_price = ((existing_price * existing_qty) + (new_price * new_qty)) / total_qty
                else:
                    weighted_avg_price = new_price
                
                # Get existing order_ids and add new one
                existing_order_ids = existing_pos.get("order_ids", [])
                new_order_id = position.get("order_id")
                if new_order_id and new_order_id not in existing_order_ids:
                    existing_order_ids.append(new_order_id)
                
                # Update position with aggregated values
                update_doc = {
                    "symbol": symbol,
                    "entry_price": weighted_avg_price,
                    "quantity": total_qty,
                    "status": status,
                    "order_ids": existing_order_ids,
                    "updated_at": datetime.utcnow(),
                }
                
                self.positions.update_one(
                    {"symbol": symbol, "status": "OPEN"},
                    {"$set": update_doc}
                )
            else:
                # New position or closing position - use position_to_doc
                # Ensure order_ids is a list
                if "order_ids" not in position:
                    order_id = position.get("order_id")
                    position["order_ids"] = [order_id] if order_id else []
                
                self.positions.update_one(
                    {"symbol": symbol},
                    {"$set": position_to_doc(position, status)},
                    upsert=True,
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
