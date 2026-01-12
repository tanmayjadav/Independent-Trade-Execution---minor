# risk/risk_manager.py

class RiskManager:
    """
    Controls capital usage and enforces max daily loss.
    """

    def __init__(self, broker, config: dict):
        self.broker = broker
        self.config = config

        self.max_daily_loss = config["risk"]["max_daily_loss"]
        
        # Position sizing configuration
        self.position_mode = config["risk"]["mode"]
        self.position_value = config["risk"]["value"]
        self.allow_multiple = config["risk"].get("allow_multiple_positions", True)

        self.opening_capital = None
        self.realized_pnl = 0.0

        self.trading_allowed = True

        self.positions = {}   # order_id -> position dict

    # -------------------------------------------------
    # Capital tracking
    # -------------------------------------------------

    def set_opening_capital(self):
        """
        Capture capital at market open.
        """
        self.opening_capital = self.broker.get_account_balance()

    def get_available_capital(self) -> float:
        """
        Remaining usable capital.
        """
        if self.opening_capital is None:
            self.set_opening_capital()

        return max(
            0.0,
            self.opening_capital + self.realized_pnl
        )

    # -------------------------------------------------
    # Trade permissions
    # -------------------------------------------------

    def can_take_new_trade(self) -> bool:
        return self.trading_allowed

    def disable_trading(self):
        self.trading_allowed = False

    # -------------------------------------------------
    # Position lifecycle
    # -------------------------------------------------

    def on_new_position(self, order_id: str, position: dict):
        """
        Called when entry order is filled.
        """
        self.positions[order_id] = position

    def on_position_closed(
        self,
        order_id: str,
        exit_price: float,
        quantity: int,
        entry_price: float = None
    ):
        """
        Called when SL / TP / square-off happens.
        """
        if order_id not in self.positions:
            return

        # Use provided entry_price if available, otherwise get from stored position
        if entry_price is None:
            entry_price = self.positions[order_id].get("entry_price")
            if entry_price is None:
                # Try entry_price_original as fallback
                entry_price = self.positions[order_id].get("entry_price_original")
                if entry_price is None:
                    # Last resort: use exit_price (results in 0 PnL, but prevents crash)
                    entry_price = exit_price

        pnl = (exit_price - entry_price) * quantity
        self.realized_pnl += pnl

        del self.positions[order_id]

        # Kill switch check
        if abs(self.realized_pnl) >= self.max_daily_loss:
            self.disable_trading()
    
    # -------------------------------------------------
    # Position sizing
    # -------------------------------------------------
    
    def calculate_quantity(
        self,
        entry_price: float,
        contract
    ) -> int:
        """
        Calculates order quantity based on capital & risk rules.
        Returns final order quantity (multiple of lot size).
        """
        available_capital = self.get_available_capital()
        lot_size = int(contract.lot_size)

        if entry_price <= 0:
            return 0

        if self.position_mode == "fixed_lot":
            qty = self.position_value * lot_size
            return qty

        if self.position_mode == "percent":
            capital_to_use = available_capital * self.position_value / 100
            max_lots = int(capital_to_use // (entry_price * lot_size))

            if max_lots <= 0:
                return 0

            return max_lots * lot_size

        raise ValueError(f"Unknown position sizing mode: {self.position_mode}")
