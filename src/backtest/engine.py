import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from ..strategy.dispersion import DispersionStrategy
import pytz

class BacktestEngine:
    def __init__(
        self,
        strategy: DispersionStrategy,
        initial_capital: float = 100000.0,
        transaction_cost: float = 0.001,  # 0.1% per trade
        slippage: float = 0.0005  # 0.05% slippage
    ):
        """
        Initialize the backtesting engine.
        
        Args:
            strategy: Strategy instance to backtest
            initial_capital: Initial capital for backtest
            transaction_cost: Transaction cost per trade (as a fraction)
            slippage: Slippage per trade (as a fraction)
        """
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.slippage = slippage
        self.positions: Dict[str, Dict] = {}
        self.trades: List[Dict] = []
        self.portfolio_value: List[float] = [initial_capital]
        self.dates: List[datetime] = []
        
    def _ensure_timezone_aware(self, dt: datetime) -> datetime:
        """
        Ensure datetime is timezone-aware.
        
        Args:
            dt: Input datetime
            
        Returns:
            Timezone-aware datetime
        """
        if dt.tzinfo is None:
            return pytz.UTC.localize(dt)
        return dt.astimezone(pytz.UTC)
    
    def run(
        self,
        prices: pd.DataFrame,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict:
        """
        Run the backtest.
        
        Args:
            prices: DataFrame with asset prices (columns are assets, index is time)
            start_date: Start date for backtest
            end_date: End date for backtest
            
        Returns:
            Dictionary with backtest results
        """
        print(f"Starting backtest with {len(prices)} price points")
        print(f"Price columns: {prices.columns.tolist()}")
        print(f"Price index range: {prices.index[0]} to {prices.index[-1]}")
        
        # Ensure prices index is timezone-aware
        if not isinstance(prices.index, pd.DatetimeIndex):
            prices.index = pd.to_datetime(prices.index)
        if prices.index.tz is None:
            prices.index = prices.index.tz_localize('UTC')
        
        # Convert start and end dates to UTC
        if start_date is not None:
            start_date = self._ensure_timezone_aware(start_date)
            prices = prices[prices.index >= start_date]
        if end_date is not None:
            end_date = self._ensure_timezone_aware(end_date)
            prices = prices[prices.index <= end_date]
        
        for date, row in prices.iterrows():
            self.dates.append(date)
            current_prices = prices.loc[:date]
            
            # Generate signals
            signals = self.strategy.generate_signals(
                current_prices,
                self.portfolio_value[-1]
            )
            
            # Update positions
            self._update_positions(signals, row)
            
            # Calculate portfolio value
            portfolio_value = self._calculate_portfolio_value(row)
            self.portfolio_value.append(portfolio_value)
            
            if len(self.portfolio_value) % 50 == 0:  # Print every 50 iterations
                print(f"Processed {len(self.portfolio_value)} dates, current portfolio value: {portfolio_value:.2f}")
        
        print(f"Backtest complete. Total dates processed: {len(self.dates)}")
        print(f"Final portfolio value: {self.portfolio_value[-1]:.2f}")
        return self._generate_results()
    
    def _update_positions(self, signals: Dict, prices: pd.Series) -> None:
        """
        Update positions based on signals.
        
        Args:
            signals: Dictionary of trading signals
            prices: Current prices for all assets
        """
        # Close positions that are no longer in signals
        for symbol in list(self.positions.keys()):
            if symbol not in signals:
                position = self.positions[symbol]
                price = prices[symbol]
                cost = self._calculate_trade_cost(
                    position['quantity'],
                    price
                )
                
                self.trades.append({
                    'date': self.dates[-1],
                    'symbol': symbol,
                    'side': 'SELL' if position['side'] == 'LONG' else 'BUY',
                    'quantity': position['quantity'],
                    'price': price,
                    'cost': cost
                })
                
                del self.positions[symbol]
        
        # Open new positions
        for symbol, signal in signals.items():
            if symbol not in self.positions:
                price = prices[symbol]
                quantity = signal['size'] / price
                cost = self._calculate_trade_cost(quantity, price)
                
                self.positions[symbol] = {
                    'quantity': quantity,
                    'side': signal['side'],
                    'entry_price': price,
                    'entry_date': self.dates[-1]
                }
                
                self.trades.append({
                    'date': self.dates[-1],
                    'symbol': symbol,
                    'side': signal['side'],
                    'quantity': quantity,
                    'price': price,
                    'cost': cost
                })
    
    def _calculate_trade_cost(self, quantity: float, price: float) -> float:
        """
        Calculate total cost of a trade including transaction costs and slippage.
        
        Args:
            quantity: Trade quantity
            price: Trade price
            
        Returns:
            float: Total cost of the trade
        """
        base_cost = quantity * price
        transaction_cost = base_cost * self.transaction_cost
        slippage_cost = base_cost * self.slippage
        return base_cost + transaction_cost + slippage_cost
    
    def _calculate_portfolio_value(self, prices: pd.Series) -> float:
        """
        Calculate current portfolio value.
        
        Args:
            prices: Current prices for all assets
            
        Returns:
            float: Current portfolio value
        """
        value = self.portfolio_value[-1]
        
        for symbol, position in self.positions.items():
            price = prices[symbol]
            if position['side'] == 'LONG':
                value += position['quantity'] * (price - position['entry_price'])
            else:
                value += position['quantity'] * (position['entry_price'] - price)
        
        return value
    
    def _generate_results(self) -> Dict:
        """
        Generate backtest results and statistics.
        
        Returns:
            Dictionary with backtest results
        """
        # Calculate returns
        returns = pd.Series(self.portfolio_value).pct_change().dropna()
        
        # Calculate statistics
        total_return = (self.portfolio_value[-1] / self.initial_capital) - 1
        annual_return = (1 + total_return) ** (252 / len(returns)) - 1
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
        
        # Calculate max drawdown
        portfolio_series = pd.Series(self.portfolio_value)
        rolling_max = portfolio_series.cummax()
        drawdowns = (rolling_max - portfolio_series) / rolling_max
        max_drawdown = float(drawdowns.max())  # Convert to float
        
        # Generate trade statistics
        trades_df = pd.DataFrame(self.trades)
        win_rate = len(trades_df[trades_df['cost'] > 0]) / len(trades_df) if len(trades_df) > 0 else 0
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'num_trades': len(self.trades),
            'portfolio_value': self.portfolio_value,
            'dates': self.dates,
            'trades': self.trades
        }
    
    def plot_results(self, save_path: Optional[str] = None) -> None:
        """
        Plot backtest results.
        
        Args:
            save_path: Optional path to save the plot
        """
        plt.figure(figsize=(12, 8))
        
        # Plot portfolio value
        plt.subplot(2, 1, 1)
        plt.plot(self.dates, self.portfolio_value[1:])
        plt.title('Portfolio Value')
        plt.grid(True)
        
        # Plot drawdown
        plt.subplot(2, 1, 2)
        portfolio_series = pd.Series(self.portfolio_value[1:], index=self.dates)
        drawdown = (portfolio_series.cummax() - portfolio_series) / portfolio_series.cummax()
        plt.plot(self.dates, drawdown)
        plt.title('Drawdown')
        plt.grid(True)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show() 