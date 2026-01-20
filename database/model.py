from datetime import datetime


# ----------------------------
# ORDER (Immutable)
# ----------------------------
def order_to_doc(order):
    return {
        "order_id": order.order_id,
        "symbol": order.contract.symbol,
        "side": order.trade_action,
        "quantity": order.order_quantity,
        "price": order.order_price,
        "status": order.status,
        "timestamp": datetime.fromtimestamp(order.create_time / 1000),
        "exchange": order.contract.exchange,
        "token": order.contract.token,
    }


# ----------------------------
# TRADE (Immutable)
# ----------------------------
def trade_to_doc(
    order_id: str,
    trade_type: str,  # "ENTRY" or "EXIT"
    price: float,  # Fill price (entry_price for ENTRY, exit_price for EXIT)
    quantity: int,
    pnl: float = 0.0,  # Only for EXIT trades
    reason: str = None,  # Only for EXIT trades (SL/TP/SQUAREOFF)
    entry_price: float = None,  # For EXIT trades, the original entry price
    exit_price: float = None,  # For EXIT trades, the exit price
    fill_number: int = 1,  # For partial fills: 1, 2, 3, etc.
):
    """
    Create trade document.
    
    For ENTRY trades:
    - trade_type = "ENTRY"
    - price = fill price
    - quantity = filled quantity
    - fill_number = which fill this is (1, 2, 3 for partial fills)
    
    For EXIT trades:
    - trade_type = "EXIT"
    - entry_price = original entry price
    - exit_price = exit price
    - quantity = exit quantity
    - pnl = calculated PnL
    - reason = "SL"/"TP"/"SQUAREOFF"
    """
    trade_id = f"{order_id}_{trade_type}_{fill_number}_{int(datetime.now().timestamp() * 1000)}"
    
    doc = {
        "trade_id": trade_id,
        "order_id": order_id,
        "trade_type": trade_type,
        "price": price,  # Fill price (for both entry and exit)
        "quantity": quantity,
        "timestamp": datetime.utcnow(),
    }
    
    if trade_type == "ENTRY":
        doc["entry_price"] = price
    elif trade_type == "EXIT":
        doc["entry_price"] = entry_price
        doc["exit_price"] = exit_price
        doc["pnl"] = pnl
        doc["reason"] = reason
    
    return doc


# ----------------------------
# POSITION (Derived)
# ----------------------------
def position_to_doc(position, status: str):
    """
    Create position document.
    Positions are aggregated by symbol, so we need to handle multiple orders
    for the same asset by calculating weighted average price.
    """
    now = datetime.utcnow()
    return {
        # identity / linkage
        "position_id": position.get("position_id"),
        "symbol": position["contract"].symbol,
        # quantities
        "quantity": position.get("quantity", 0),  # Net open qty (remaining)
        "opened_quantity": position.get("opened_quantity"),
        "closed_quantity": position.get("closed_quantity"),
        # pricing
        "entry_price": position.get("entry_price"),  # Weighted average for multiple orders
        "exit_price": position.get("exit_price"),
        "last_price": position.get("last_price"),
        # PnL
        "realized_pnl": position.get("realized_pnl"),
        "unrealized_pnl": position.get("unrealized_pnl"),
        "net_pnl": position.get("net_pnl"),
        # lifecycle
        "status": status,
        "order_ids": position.get("order_ids", []),  # Entry order_ids that contributed
        "exit_order_ids": position.get("exit_order_ids", []),
        "created_at": position.get("created_at", now),
        "updated_at": now,
        "closed_at": position.get("closed_at"),
    }


# ----------------------------
# DAILY SUMMARY
# ----------------------------
def daily_summary_to_doc(
    date,
    total_trades,
    wins,
    losses,
    net_pnl,
    max_drawdown
):
    """
    Convert daily summary to MongoDB document.
    Converts date (datetime.date) to datetime.datetime for MongoDB compatibility.
    """
    # Convert date to datetime if it's a date object
    if isinstance(date, datetime):
        date_dt = date
    else:
        # Convert date to datetime at midnight UTC
        from datetime import date as date_type
        if isinstance(date, date_type):
            date_dt = datetime.combine(date, datetime.min.time())
        else:
            # If it's already a string or other format, try to keep it as is
            date_dt = date
    
    return {
        "date": date_dt,  # Use datetime object for MongoDB
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "net_pnl": net_pnl,
        "max_drawdown": max_drawdown,
        "created_at": datetime.utcnow(),
    }
