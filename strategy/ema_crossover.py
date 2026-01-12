# strategy/ema_crossover.py

from typing import Optional


class EMACrossoverStrategy:
    """
    EMA Crossover Strategy for NIFTY options.
    
    Generates signals:
    - BUY_CE when fast EMA crosses above slow EMA (bullish)
    - BUY_PE when fast EMA crosses below slow EMA (bearish)
    """

    def __init__(self, fast_period: int = 9, slow_period: int = 26):
        """
        Args:
            fast_period: Fast EMA period (default 9)
            slow_period: Slow EMA period (default 26)
        """
        self.fast_period = fast_period
        self.slow_period = slow_period

        # EMA values
        self.fast_ema = None
        self.slow_ema = None
        self.prev_fast_ema = None
        self.prev_slow_ema = None

        # Candle history for EMA calculation
        self.candles = []

    def on_candle(self, candle: dict) -> Optional[str]:
        """
        Called when a new candle closes.
        
        Args:
            candle: Dict with keys: open, high, low, close, timestamp
            
        Returns:
            "BUY_CE" if bullish crossover detected
            "BUY_PE" if bearish crossover detected
            None if no signal
        """
        if not candle or "close" not in candle:
            return None

        close_price = float(candle["close"])

        # Add candle to history
        self.candles.append(close_price)

        # Need enough candles to calculate both EMAs
        if len(self.candles) < self.slow_period:
            return None

        # Calculate EMAs
        self.prev_fast_ema = self.fast_ema
        self.prev_slow_ema = self.slow_ema

        self.fast_ema = self._calculate_ema(
            self.candles[-self.fast_period:],
            self.fast_period,
            self.prev_fast_ema
        )
        self.slow_ema = self._calculate_ema(
            self.candles[-self.slow_period:],
            self.slow_period,
            self.prev_slow_ema
        )

        # Need previous values to detect crossover
        if self.prev_fast_ema is None or self.prev_slow_ema is None:
            return None

        # Detect crossover
        # Bullish: fast EMA crosses above slow EMA
        if (self.prev_fast_ema <= self.prev_slow_ema and 
            self.fast_ema > self.slow_ema):
            return "BUY_CE"

        # Bearish: fast EMA crosses below slow EMA
        if (self.prev_fast_ema >= self.prev_slow_ema and 
            self.fast_ema < self.slow_ema):
            return "BUY_PE"

        return None

    def _calculate_ema(self, prices: list, period: int, prev_ema: Optional[float]) -> float:
        """
        Calculate Exponential Moving Average.
        
        Args:
            prices: List of closing prices
            period: EMA period
            prev_ema: Previous EMA value (for incremental calculation)
            
        Returns:
            EMA value
        """
        if prev_ema is None:
            # First EMA = Simple Moving Average
            return sum(prices) / len(prices)
        
        # EMA formula: EMA = (Price - PrevEMA) * Multiplier + PrevEMA
        # Multiplier = 2 / (Period + 1)
        multiplier = 2.0 / (period + 1)
        current_price = prices[-1]

        return (current_price - prev_ema) * multiplier + prev_ema
