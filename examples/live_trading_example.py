import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.dispersion import DispersionStrategy
from src.broker.ibkr_client import IBKRClient
from src.utils.data_loader import DataLoader
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/live_trading.log'),
        logging.StreamHandler()
    ]
)

def main():
    # Initialize components
    data_loader = DataLoader(cache_dir='data/cache')
    strategy = DispersionStrategy(
        lookback_period=20,
        min_correlation=0.7,
        max_position_size=0.1,
        rebalance_frequency='1D'
    )
    client = IBKRClient()
    
    # Define symbols to trade
    symbols = [
        'SPY',  # S&P 500 ETF
        'QQQ',  # NASDAQ-100 ETF
        'IWM',  # Russell 2000 ETF
        'DIA',  # Dow Jones ETF
        'EFA',  # EAFE ETF
        'EEM',  # Emerging Markets ETF
        'TLT',  # 20+ Year Treasury Bond ETF
        'GLD',  # Gold ETF
        'VNQ',  # Real Estate ETF
        'XLE'   # Energy Sector ETF
    ]
    
    try:
        # Connect to IBKR
        logging.info("Connecting to IBKR...")
        if not client.connect():
            logging.error("Failed to connect to IBKR")
            return
        
        logging.info("Successfully connected to IBKR")
        
        # Main trading loop
        while True:
            try:
                # Get current market data
                logging.info("Fetching market data...")
                prices = client.get_market_data(symbols)
                
                if prices is None or prices.empty:
                    logging.warning("No market data received")
                    time.sleep(60)
                    continue
                
                # Get current portfolio value
                portfolio_value = client.get_portfolio_value()
                logging.info(f"Current portfolio value: ${portfolio_value:,.2f}")
                
                # Generate signals
                logging.info("Generating trading signals...")
                signals = strategy.generate_signals(prices, portfolio_value)
                
                # Execute trades
                for symbol, signal in signals.items():
                    current_position = client.positions.get(symbol)
                    
                    if current_position is None:
                        # Open new position
                        logging.info(f"Opening {signal['side']} position in {symbol}")
                        order_id = client.place_order(
                            symbol=symbol,
                            quantity=signal['size'],
                            side=signal['side']
                        )
                        if order_id:
                            logging.info(f"Order placed successfully. Order ID: {order_id}")
                    else:
                        # Check if we need to close position
                        if current_position.side != signal['side']:
                            logging.info(f"Closing position in {symbol}")
                            if client.close_position(symbol):
                                logging.info(f"Position closed successfully")
                
                # Sleep until next rebalance
                sleep_time = strategy._get_sleep_time()
                logging.info(f"Sleeping for {sleep_time} seconds until next rebalance")
                time.sleep(sleep_time)
                
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait a minute before retrying
    
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        # Disconnect from IBKR
        client.disconnect()
        logging.info("Disconnected from IBKR")

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/cache', exist_ok=True)
    
    main() 