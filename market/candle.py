# market/candle_aggregator.py

from datetime import datetime
from typing import Optional


class CandleAggregator:
    """
    Converts tick data into fixed timeframe candles (1-minute).

    Plug-and-play with variance_connect:
    - Input  : OnTickDataModel
    - Output : dict (closed candle)
    """

    def __init__(self, timeframe_sec: int = 60):
        self.timeframe_sec = timeframe_sec

        self.current_candle = None
        self.current_bucket = None

    def _get_bucket(self, ts_ms: int) -> int:
        """
        Returns bucket timestamp (epoch seconds)
        Example: 09:15:34 -> 09:15:00
        """
        ts_sec = ts_ms // 1000
        return ts_sec - (ts_sec % self.timeframe_sec)

    def on_tick(self, tick) -> Optional[dict]:
        """
        Call this on every OnTickDataModel event.

        Returns:
            - closed candle (dict) when candle completes
            - None otherwise
        """

        if tick is None or tick.ltp is None:
            return None

        # Get timestamp - try different possible attribute names
        if hasattr(tick, 'ts'):
            ts_ms = tick.ts
        elif hasattr(tick, 'timestamp'):
            ts_ms = tick.timestamp
        else:
            # Fallback: use current time in milliseconds
            import time
            ts_ms = int(time.time() * 1000)
        
        price = float(tick.ltp)
        symbol = tick.contract.symbol if hasattr(tick, 'contract') and tick.contract else "UNKNOWN"

        bucket = self._get_bucket(ts_ms)

        # First tick ever
        if self.current_bucket is None:
            self._start_new_candle(bucket, ts_ms, price, symbol)
            return None

        # Same candle
        if bucket == self.current_bucket:
            self._update_candle(price)
            return None

        # Candle closed → emit it
        closed_candle = self.current_candle

        # Start new candle
        self._start_new_candle(bucket, ts_ms, price, symbol)

        return closed_candle

    def _start_new_candle(self, bucket: int, ts_ms: int, price: float, symbol: str):
        self.current_bucket = bucket

        self.current_candle = {
            "symbol": symbol,
            "timestamp": bucket * 1000,  # candle close time
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }

    def _update_candle(self, price: float):
        self.current_candle["high"] = max(self.current_candle["high"], price)
        self.current_candle["low"] = min(self.current_candle["low"], price)
        self.current_candle["close"] = price
