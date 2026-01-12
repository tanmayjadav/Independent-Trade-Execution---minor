# market/market_clock.py

from datetime import datetime, time, timedelta
from typing import Optional, Tuple
import time as time_module


class MarketClock:
    """
    Market timing helper.
    Supports configurable market hours.
    """

    # Default NSE timings (used if config not provided)
    DEFAULT_MARKET_OPEN = time(9, 15)
    DEFAULT_MARKET_CLOSE = time(15, 15)

    # Instance variables for configurable timings
    _market_open: Optional[time] = None
    _market_close: Optional[time] = None
    _market_open_str: Optional[str] = None
    _market_close_str: Optional[str] = None

    @classmethod
    def configure(cls, market_open: str, market_close: str):
        """
        Configure market timings.
        
        Args:
            market_open: Time string in "HH:MM" format (e.g., "09:15")
            market_close: Time string in "HH:MM" format (e.g., "15:15")
        """
        h, m = map(int, market_open.split(":"))
        cls._market_open = time(h, m)
        cls._market_open_str = market_open
        
        h, m = map(int, market_close.split(":"))
        cls._market_close = time(h, m)
        cls._market_close_str = market_close

    @classmethod
    def get_market_open(cls) -> time:
        """Get market open time (configurable or default)."""
        return cls._market_open if cls._market_open is not None else cls.DEFAULT_MARKET_OPEN

    @classmethod
    def get_market_close(cls) -> time:
        """Get market close time (configurable or default)."""
        return cls._market_close if cls._market_close is not None else cls.DEFAULT_MARKET_CLOSE

    @classmethod
    def get_market_hours_str(cls) -> str:
        """Get formatted market hours string (e.g., "09:15 - 15:20")."""
        open_str = cls._market_open_str if cls._market_open_str else "09:15"
        close_str = cls._market_close_str if cls._market_close_str else "15:15"
        return f"{open_str} - {close_str}"

    @staticmethod
    def is_weekend() -> bool:
        """Check if current day is weekend (Saturday or Sunday)."""
        return datetime.now().weekday() >= 5

    @classmethod
    def is_market_open(cls) -> bool:
        """
        Check if market is currently open.
        Uses configured timings if set, otherwise uses defaults.
        """
        now = datetime.now().time()
        market_open = cls.get_market_open()
        market_close = cls.get_market_close()
        return market_open <= now <= market_close

    @classmethod
    def get_time_until_next_open(cls) -> Tuple[int, int]:
        """
        Calculate time until next market open.
        
        Returns:
            Tuple of (hours, minutes) until next market open
        """
        now = datetime.now()
        market_open = cls.get_market_open()
        market_open_time = now.replace(hour=market_open.hour, minute=market_open.minute, second=0, microsecond=0)
        
        # If market open time has passed today, set it for tomorrow
        if market_open_time <= now:
            market_open_time += timedelta(days=1)
        
        time_until_open = market_open_time - now
        hours = int(time_until_open.total_seconds() // 3600)
        minutes = int((time_until_open.total_seconds() % 3600) // 60)
        return hours, minutes

    @classmethod
    def format_time_until_open(cls) -> str:
        """Format time until next market open as string (e.g., "2h 30m")."""
        hours, minutes = cls.get_time_until_next_open()
        return f"{hours}h {minutes}m"

    @classmethod
    def wait_for_market_open(cls, check_interval: int = 60, verbose: bool = True):
        """
        Wait until market opens. Blocks until market is open.
        
        Args:
            check_interval: Seconds between checks (default: 60)
            verbose: Whether to print status messages (default: True)
        """
        if verbose:
            print(f"\nMarket is currently CLOSED")
            print(f"   Market hours: {cls.get_market_hours_str()}")
            print(f"   Current time: {datetime.now().strftime('%H:%M:%S')}")
            print(f"   Next market open in {cls.format_time_until_open()}")
            print(f"   Waiting for market to open before starting data stream...\n")
        
        # Wait until market opens
        while not cls.is_market_open():
            time_module.sleep(check_interval)
            if cls.is_market_open():
                if verbose:
                    print(f"Market is now OPEN. Starting data stream...\n")
                break

    @staticmethod
    def is_squareoff_time(squareoff: str) -> bool:
        """
        Check if squareoff time has been reached.
        
        Args:
            squareoff: Time string in "HH:MM" format (e.g., "15:15")
        """
        h, m = map(int, squareoff.split(":"))
        return datetime.now().time() >= time(h, m)
