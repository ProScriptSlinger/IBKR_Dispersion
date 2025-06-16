import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.dispersion import DispersionStrategy
from src.backtest.engine import BacktestEngine
from src.utils.data_loader import DataLoader
import pandas as pd
from datetime import datetime, timedelta
import pytz

def main():
    # Initialize components
    data_loader = DataLoader(cache_dir='data/cache')
    strategy = DispersionStrategy(
        lookback_period=20,
        min_correlation=0.7,
        max_position_size=0.1,
        rebalance_frequency='1D'
    )
    backtest_engine = BacktestEngine(
        strategy=strategy,
        initial_capital=100000.0,
        transaction_cost=0.001,
        slippage=0.0005
    )
    
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
    
    # Fetch and preprocess data
    end_date = datetime.now(pytz.UTC)
    start_date = end_date - timedelta(days=365)  # 1 year of data
    
    print("Fetching historical data...")
    data = data_loader.fetch_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval='1d'
    )
    
    print(f"Data shape: {data.shape}")
    print(f"Data columns: {data.columns.tolist()}")
    print(f"Data index range: {data.index[0]} to {data.index[-1]}")
    
    print("Preprocessing data...")
    data = data_loader.preprocess_data(data)
    
    print(f"Preprocessed data shape: {data.shape}")
    print(f"Preprocessed data columns: {data.columns.tolist()}")
    print(f"Preprocessed data index range: {data.index[0]} to {data.index[-1]}")
    
    # Run backtest
    print("Running backtest...")
    results = backtest_engine.run(
        prices=data,
        start_date=start_date,
        end_date=end_date
    )
    
    # Print results
    print("\nBacktest Results:")
    print(f"Total Return: {results['total_return']:.2%}")
    print(f"Annual Return: {results['annual_return']:.2%}")
    print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['max_drawdown']:.2%}")
    print(f"Win Rate: {results['win_rate']:.2%}")
    print(f"Number of Trades: {results['num_trades']}")
    
    # Plot results
    print("\nGenerating plots...")
    backtest_engine.plot_results(save_path='results/backtest_results.png')
    
    # Save trade history
    trades_df = pd.DataFrame(results['trades'])
    trades_df.to_csv('results/trade_history.csv')
    
    print("\nBacktest complete! Results saved to 'results/' directory.")

if __name__ == "__main__":
    main() 