import os
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
from ib_insync import *
from dotenv import load_dotenv

class IBKRClient:
    def __init__(self):
        """Initialize IBKR client with environment variables."""
        load_dotenv()
        self.ib = IB()
        self.connected = False
        self.positions: Dict[str, Position] = {}
        self.market_data: Dict[str, pd.DataFrame] = {}
        
    def connect(self) -> bool:
        """
        Connect to IBKR TWS/Gateway.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.ib.connect(
                host=os.getenv('IBKR_HOST', '127.0.0.1'),
                port=int(os.getenv('IBKR_PORT', '7497')),
                clientId=int(os.getenv('IBKR_CLIENT_ID', '1'))
            )
            self.connected = True
            return True
        except Exception as e:
            print(f"Failed to connect to IBKR: {e}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from IBKR."""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
    
    def get_market_data(
        self,
        symbols: List[str],
        duration: str = '1 D',
        bar_size: str = '1 min'
    ) -> pd.DataFrame:
        """
        Get historical market data for specified symbols.
        
        Args:
            symbols: List of symbols to fetch data for
            duration: Duration of historical data
            bar_size: Bar size for historical data
            
        Returns:
            DataFrame with market data
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        data = {}
        for symbol in symbols:
            contract = Stock(symbol, 'SMART', 'USD')
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=True
            )
            
            if bars:
                df = util.df(bars)
                data[symbol] = df['close']
        
        return pd.DataFrame(data)
    
    def get_portfolio_value(self) -> float:
        """
        Get current portfolio value.
        
        Returns:
            float: Current portfolio value
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        account = self.ib.managedAccounts()[0]
        portfolio = self.ib.portfolio()
        
        # Calculate total portfolio value
        total_value = 0
        for position in portfolio:
            total_value += position.position * position.avgCost
        
        # Add cash
        account_summary = self.ib.accountSummary()
        for summary in account_summary:
            if summary.tag == 'NetLiquidation':
                total_value += float(summary.value)
                break
        
        return total_value
    
    def place_order(
        self,
        symbol: str,
        quantity: float,
        side: str,
        order_type: str = 'MKT'
    ) -> Optional[int]:
        """
        Place an order.
        
        Args:
            symbol: Symbol to trade
            quantity: Quantity to trade
            side: 'BUY' or 'SELL'
            order_type: Order type (default: 'MKT')
            
        Returns:
            Optional[int]: Order ID if successful, None otherwise
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            order = MarketOrder(side, quantity)
            trade = self.ib.placeOrder(contract, order)
            
            # Wait for order to fill
            while not trade.isDone():
                self.ib.sleep(0.1)
            
            return trade.order.orderId
        except Exception as e:
            print(f"Error placing order: {e}")
            return None
    
    def close_position(self, symbol: str) -> bool:
        """
        Close a position.
        
        Args:
            symbol: Symbol to close position for
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            position = self.ib.positions(contract)[0]
            
            if position:
                # Create opposite order to close position
                side = 'SELL' if position.position > 0 else 'BUY'
                quantity = abs(position.position)
                
                order = MarketOrder(side, quantity)
                trade = self.ib.placeOrder(contract, order)
                
                # Wait for order to fill
                while not trade.isDone():
                    self.ib.sleep(0.1)
                
                return True
        except Exception as e:
            print(f"Error closing position: {e}")
        
        return False
    
    def get_last_price(self, symbol: str) -> Optional[float]:
        """
        Get the last traded price for a symbol.
        
        Args:
            symbol: Symbol to get price for
            
        Returns:
            Optional[float]: Last traded price if available
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            ticker = self.ib.reqMktData(contract)
            self.ib.sleep(1)  # Wait for data
            
            if ticker.last:
                return ticker.last
        except Exception as e:
            print(f"Error getting last price: {e}")
        
        return None 