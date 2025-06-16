import pandas as pd
import numpy as np
import yfinance as yf
from typing import List, Optional, Union
from datetime import datetime, timedelta
import logging
from .network_utils import verify_yahoo_finance_connectivity
import pytz

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize the data loader.
        
        Args:
            cache_dir: Optional directory to cache downloaded data
        """
        self.cache_dir = cache_dir
    
    def fetch_data(
        self,
        symbols: List[str],
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        interval: str = '1d'
    ) -> pd.DataFrame:
        """
        Fetch historical data for multiple symbols.
        
        Args:
            symbols: List of symbols to fetch data for
            start_date: Start date for data
            end_date: End date for data
            interval: Data interval (e.g., '1d', '1h', '1m')
            
        Returns:
            DataFrame with historical data
        """
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        
        # Ensure timezone awareness
        if start_date.tzinfo is None:
            start_date = pytz.UTC.localize(start_date)
        if end_date.tzinfo is None:
            end_date = pytz.UTC.localize(end_date)
        
        # Check cache first
        if self.cache_dir:
            cache_file = f"{self.cache_dir}/{'-'.join(symbols)}_{start_date.date()}_{end_date.date()}_{interval}.csv"
            try:
                df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                # Ensure index is timezone-aware
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC')
                return df
            except FileNotFoundError:
                pass
        
        # Verify connectivity before attempting to download
        if not verify_yahoo_finance_connectivity():
            logger.error("Failed to verify connectivity to Yahoo Finance. Please check your network settings and DNS configuration.")
            raise ConnectionError("Failed to connect to Yahoo Finance. Check DNS settings and PiHole configuration if applicable.")
        
        # Download data
        data = {}
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(
                    start=start_date,
                    end=end_date,
                    interval=interval
                )
                if df.empty:
                    logger.warning(f"No data received for {symbol}")
                    continue
                data[symbol] = df['Close']
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                raise
        
        if not data:
            raise ValueError("No data was successfully downloaded for any symbols")
        
        # Combine data
        combined_data = pd.DataFrame(data)
        
        # Ensure index is timezone-aware
        if combined_data.index.tz is None:
            combined_data.index = combined_data.index.tz_localize('UTC')
        
        # Cache data
        if self.cache_dir:
            combined_data.to_csv(cache_file)
        
        return combined_data
    
    def preprocess_data(
        self,
        data: pd.DataFrame,
        fill_method: str = 'ffill',
        min_periods: int = 20
    ) -> pd.DataFrame:
        """
        Preprocess the data by handling missing values and outliers.
        
        Args:
            data: DataFrame with price data
            fill_method: Method to fill missing values ('ffill', 'bfill', 'interpolate')
            min_periods: Minimum number of periods required for valid data
            
        Returns:
            Preprocessed DataFrame
        """
        # Fill missing values
        if fill_method == 'ffill':
            data = data.ffill()
        elif fill_method == 'bfill':
            data = data.bfill()
        elif fill_method == 'interpolate':
            data = data.interpolate()
        
        # Remove rows with too many missing values
        data = data.dropna(thresh=min_periods)
        
        # Handle outliers using z-score
        for column in data.columns:
            z_scores = np.abs((data[column] - data[column].mean()) / data[column].std())
            data.loc[z_scores > 3, column] = np.nan
        
        # Fill remaining missing values
        data = data.ffill()
        
        return data
    
    def calculate_returns(
        self,
        data: pd.DataFrame,
        method: str = 'log'
    ) -> pd.DataFrame:
        """
        Calculate returns from price data.
        
        Args:
            data: DataFrame with price data
            method: Return calculation method ('log' or 'simple')
            
        Returns:
            DataFrame with returns
        """
        if method == 'log':
            return np.log(data / data.shift(1))
        else:
            return data.pct_change()
    
    def calculate_volatility(
        self,
        data: pd.DataFrame,
        window: int = 20
    ) -> pd.DataFrame:
        """
        Calculate rolling volatility.
        
        Args:
            data: DataFrame with price data
            window: Rolling window size
            
        Returns:
            DataFrame with volatility
        """
        returns = self.calculate_returns(data)
        return returns.rolling(window=window).std() * np.sqrt(252)
    
    def calculate_correlation(
        self,
        data: pd.DataFrame,
        window: int = 20
    ) -> pd.DataFrame:
        """
        Calculate rolling correlation matrix.
        
        Args:
            data: DataFrame with price data
            window: Rolling window size
            
        Returns:
            DataFrame with correlation matrix
        """
        returns = self.calculate_returns(data)
        return returns.rolling(window=window).corr() 