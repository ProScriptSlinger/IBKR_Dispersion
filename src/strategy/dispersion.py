import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class Position:
    symbol: str
    quantity: float
    entry_price: float
    entry_time: datetime
    side: str  # 'LONG' or 'SHORT'

class DispersionStrategy:
    def __init__(
        self,
        lookback_period: int = 20,
        min_correlation: float = 0.7,
        max_position_size: float = 0.1,
        rebalance_frequency: str = '1D'
    ):
        """
        Initialize the dispersion trading strategy.
        
        Args:
            lookback_period: Number of days to look back for calculations
            min_correlation: Minimum correlation threshold for pair selection
            max_position_size: Maximum position size as a fraction of portfolio
            rebalance_frequency: How often to rebalance positions ('1D', '1W', etc.)
        """
        self.lookback_period = lookback_period
        self.min_correlation = min_correlation
        self.max_position_size = max_position_size
        self.rebalance_frequency = rebalance_frequency
        self.positions: Dict[str, Position] = {}
        
    def calculate_dispersion(self, prices: pd.DataFrame) -> pd.Series:
        """
        Calculate the cross-sectional dispersion of returns.
        
        Args:
            prices: DataFrame with asset prices (columns are assets, index is time)
            
        Returns:
            Series of dispersion values
        """
        returns = prices.pct_change()
        return returns.std(axis=1)
    
    def find_correlated_pairs(
        self,
        prices: pd.DataFrame,
        min_correlation: float = None
    ) -> List[Tuple[str, str, float]]:
        """
        Find pairs of assets with high correlation.
        
        Args:
            prices: DataFrame with asset prices
            min_correlation: Minimum correlation threshold (overrides instance default)
            
        Returns:
            List of tuples (asset1, asset2, correlation)
        """
        if min_correlation is None:
            min_correlation = self.min_correlation
            
        returns = prices.pct_change().dropna()
        corr_matrix = returns.corr()
        
        pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) >= min_correlation:
                    pairs.append((
                        corr_matrix.columns[i],
                        corr_matrix.columns[j],
                        corr
                    ))
        
        return sorted(pairs, key=lambda x: abs(x[2]), reverse=True)
    
    def calculate_position_sizes(
        self,
        prices: pd.DataFrame,
        portfolio_value: float
    ) -> Dict[str, float]:
        """
        Calculate position sizes based on volatility targeting.
        
        Args:
            prices: DataFrame with asset prices
            portfolio_value: Current portfolio value
            
        Returns:
            Dictionary mapping symbols to position sizes
        """
        returns = prices.pct_change().dropna()
        volatilities = returns.std()
        
        # Inverse volatility weighting
        weights = 1 / volatilities
        weights = weights / weights.sum()
        
        # Apply maximum position size constraint
        weights = np.minimum(weights, self.max_position_size)
        weights = weights / weights.sum()
        
        return {symbol: weight * portfolio_value for symbol, weight in weights.items()}
    
    def generate_signals(
        self,
        prices: pd.DataFrame,
        portfolio_value: float
    ) -> Dict[str, Dict[str, float]]:
        """
        Generate trading signals based on dispersion metrics.
        
        Args:
            prices: DataFrame with asset prices
            portfolio_value: Current portfolio value
            
        Returns:
            Dictionary of trading signals with position sizes
        """
        dispersion = self.calculate_dispersion(prices)
        pairs = self.find_correlated_pairs(prices)
        position_sizes = self.calculate_position_sizes(prices, portfolio_value)
        
        signals = {}
        for asset1, asset2, corr in pairs:
            # Calculate relative strength
            returns1 = prices[asset1].pct_change()
            returns2 = prices[asset2].pct_change()
            spread = returns1 - returns2
            
            # Generate signals based on spread z-score
            z_score = (spread - spread.mean()) / spread.std()
            
            if z_score.iloc[-1] > 2:  # Short asset1, long asset2
                signals[asset1] = {
                    'side': 'SHORT',
                    'size': position_sizes[asset1]
                }
                signals[asset2] = {
                    'side': 'LONG',
                    'size': position_sizes[asset2]
                }
            elif z_score.iloc[-1] < -2:  # Long asset1, short asset2
                signals[asset1] = {
                    'side': 'LONG',
                    'size': position_sizes[asset1]
                }
                signals[asset2] = {
                    'side': 'SHORT',
                    'size': position_sizes[asset2]
                }
        
        return signals
    
    def run_live(self, broker_client) -> None:
        """
        Run the strategy in live trading mode.
        
        Args:
            broker_client: Broker client instance for order execution
        """
        while True:
            try:
                # Get current market data
                prices = broker_client.get_market_data()
                portfolio_value = broker_client.get_portfolio_value()
                
                # Generate signals
                signals = self.generate_signals(prices, portfolio_value)
                
                # Execute trades
                for symbol, signal in signals.items():
                    if symbol not in self.positions:
                        # Open new position
                        order_id = broker_client.place_order(
                            symbol=symbol,
                            quantity=signal['size'],
                            side=signal['side']
                        )
                        if order_id:
                            self.positions[symbol] = Position(
                                symbol=symbol,
                                quantity=signal['size'],
                                entry_price=broker_client.get_last_price(symbol),
                                entry_time=datetime.now(),
                                side=signal['side']
                            )
                    else:
                        # Check if we need to close position
                        position = self.positions[symbol]
                        if position.side != signal['side']:
                            broker_client.close_position(symbol)
                            del self.positions[symbol]
                
                # Sleep until next rebalance
                time.sleep(self._get_sleep_time())
                
            except Exception as e:
                print(f"Error in live trading: {e}")
                time.sleep(60)  # Wait a minute before retrying
    
    def _get_sleep_time(self) -> int:
        """Calculate sleep time until next rebalance."""
        if self.rebalance_frequency == '1D':
            return 24 * 60 * 60
        elif self.rebalance_frequency == '1W':
            return 7 * 24 * 60 * 60
        else:
            return 60 * 60  # Default to 1 hour 