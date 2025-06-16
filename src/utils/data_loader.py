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
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Fetch historical data for multiple symbols.
        
        Args:
            symbols: List of symbols to fetch data for
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with historical data
        """
        # Ensure dates are timezone-aware
        if start_date.tzinfo is None:
            start_date = pytz.UTC.localize(start_date)
        if end_date.tzinfo is None:
            end_date = pytz.UTC.localize(end_date)
        
        # Convert dates to UTC
        start_date = start_date.astimezone(pytz.UTC)
        end_date = end_date.astimezone(pytz.UTC)
        
        # Fetch data for each symbol
        dfs = []
        for symbol in symbols:
            try:
                df = yf.download(
                    symbol,
                    start=start_date,
                    end=end_date,
                    progress=False
                )
                
                # Ensure the index is a DatetimeIndex
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                
                # Convert index to UTC if it has timezone info
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_convert('UTC')
                else:
                    df.index = df.index.tz_localize('UTC')
                
                # Add symbol column
                df['Symbol'] = symbol
                dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        if not dfs:
            raise ValueError("No data could be fetched for any symbol")
        
        # Combine all dataframes
        df = pd.concat(dfs)
        
        # Pivot to get symbols as columns
        df = df.pivot(columns='Symbol', values='Close')
        
        return df
    
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