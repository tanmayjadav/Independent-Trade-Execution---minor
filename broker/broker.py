# broker/broker_factory.py

from broker.paper_broker import PaperBroker
from variance_connect.brokers import XTS, AngelOne


class BrokerFactory:
    """
    Creates broker instances based on config.
    Returns (trading_broker, market_data_broker) tuple.
    """

    @staticmethod
    def create(config: dict, credentials: dict):
        """
        Creates both trading and market data brokers.
        Returns: (trading_broker, market_data_broker)
        """
        is_paper = config["deployment"]["paper_trading"]
        
        # Create trading broker
        if is_paper:
            starting_capital = config["deployment"].get("paper_capital", 1_000_000)
            trading_broker = PaperBroker(starting_capital=starting_capital)
        else:
            trading_broker = XTS(credentials=credentials, data={})
        
        # Create market data broker
        if is_paper:
            # Paper trading: use Angel One for market data
            angel_credentials = {
                "client_code": credentials.get("client_code"),
                "api_key": credentials.get("api_key"),
                "password": credentials.get("password"),
                "totp_key": credentials.get("totp_key")
            }
            
            # Validate Angel One credentials
            if not all([angel_credentials.get("client_code"), 
                       angel_credentials.get("api_key"),
                       angel_credentials.get("password"),
                       angel_credentials.get("totp_key")]):
                raise Exception("Angel One credentials not found in credentials.json. Required: client_code, api_key, password, totp_key")
            
            market_data_broker = AngelOne(credentials=angel_credentials, data={})
        else:
            # Live trading: use same broker for both
            market_data_broker = trading_broker
        
        return trading_broker, market_data_broker
