# execution/option_selector.py

from datetime import datetime
from typing import Optional
from utils.logger import get_component_logger


class OptionSelector:
    """
    Selects ATM option contract based on signal.
    Fetches full option chain from Angel One (via InstrumentManager which uses REST API cached data)
    and selects the contract with strike nearest to spot price.

    How it works:
    1. Instruments are fetched via Angel One REST API (md_broker.get_instruments()) at startup
    2. Instruments are cached in InstrumentManager
    3. get_options_chain() filters from cached instruments (no additional REST API call needed)
    
    Input  : signal, spot price
    Output : variance_connect Contract
    """

    def __init__(self, instrument_manager, underlying_contract=None):
        self.im = instrument_manager
        self.underlying_contract = underlying_contract
        self.logger = get_component_logger("option_selector")

    def select(
        self,
        signal: str,
        spot_price: float
    ) -> Optional[object]:
        """
        Fetches full option chain from cached Angel One instruments and returns contract with strike nearest to spot price.
        Returns Contract or None
        
        Note: Instruments are fetched via Angel One REST API at startup and cached in InstrumentManager.
        This method filters from the cached data (no additional REST API call).
        """

        if signal not in ("BUY_CE", "BUY_PE"):
            return None

        if not self.underlying_contract:
            raise Exception("Underlying contract not provided to OptionSelector")

        # Get full option chain for nearest expiry (index 0 = nearest)
        # This uses cached instruments fetched via Angel One REST API at startup
        try:
            options_chain = self.im.get_options_chain(
                underlying_contract=self.underlying_contract,
                expiry=0,  # 0 = nearest expiry
                expiry_type="ALL"
            )
        except Exception as e:
            self.logger.error(f"Failed to fetch option chain: {e}", exc_info=True)
            return None

        if not options_chain or not options_chain.contracts:
            self.logger.warning("No contracts found in option chain")
            return None

        # Determine option type
        option_type = "CE" if signal == "BUY_CE" else "PE"

        # Filter contracts by option type (CE or PE)
        # Note: variance_connect uses 'call_put' attribute, not 'instrument_type'
        filtered_contracts = [
            contract for contract in options_chain.contracts
            if (hasattr(contract, 'call_put') and contract.call_put == option_type) or
               (hasattr(contract, 'instrument_type') and contract.instrument_type == option_type)
        ]

        if not filtered_contracts:
            self.logger.warning(f"No {option_type} contracts found in option chain")
            return None

        # Find contract with strike nearest to spot price
        nearest_contract = None
        min_distance = float('inf')

        for contract in filtered_contracts:
            # variance_connect uses 'strike_price' attribute
            strike = contract.strike_price if hasattr(contract, 'strike_price') else (contract.strike if hasattr(contract, 'strike') else None)
            if strike is None:
                continue
            distance = abs(strike - spot_price)
            
            if distance < min_distance:
                min_distance = distance
                nearest_contract = contract

        if nearest_contract:
            strike_val = nearest_contract.strike_price if hasattr(nearest_contract, 'strike_price') else (nearest_contract.strike if hasattr(nearest_contract, 'strike') else 'N/A')
            self.logger.info(f"Selected {option_type} contract: Strike {strike_val} (spot: {spot_price:.2f}, distance: {min_distance:.2f})")
        
        return nearest_contract
