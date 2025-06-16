# Dispersion Trading Strategy

This project implements a dispersion trading strategy for indices and ETFs, with backtesting capabilities and live trading integration through Interactive Brokers (IBKR) API.

## Project Structure

```
├── src/
│   ├── strategy/
│   │   ├── __init__.py
│   │   ├── dispersion.py        # Core dispersion strategy implementation
│   │   └── indicators.py        # Technical indicators and calculations
│   ├── backtest/
│   │   ├── __init__.py
│   │   └── engine.py           # Backtesting framework
│   ├── broker/
│   │   ├── __init__.py
│   │   └── ibkr_client.py      # IBKR API integration
│   └── utils/
│       ├── __init__.py
│       └── data_loader.py      # Data fetching and preprocessing
├── tests/
│   ├── __init__.py
│   ├── test_strategy.py
│   └── test_backtest.py
├── notebooks/
│   └── strategy_analysis.ipynb  # Jupyter notebook for analysis
├── requirements.txt
└── README.md
```

## Setup

1. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up IBKR credentials:
   Create a `.env` file in the root directory with:

```
IBKR_HOST=127.0.0.1
IBKR_PORT=7497  # 7496 for TWS, 7497 for IB Gateway
IBKR_CLIENT_ID=1
```

## Features

- Dispersion Strategy Implementation

  - Cross-sectional volatility calculation
  - Pair selection based on dispersion metrics
  - Position sizing and risk management

- Backtesting Framework

  - Historical performance analysis
  - Risk metrics calculation
  - Transaction cost simulation

- IBKR Integration
  - Real-time market data streaming
  - Order execution
  - Portfolio management

## Usage

1. Backtesting:

```python
from src.backtest.engine import BacktestEngine
from src.strategy.dispersion import DispersionStrategy

# Initialize and run backtest
engine = BacktestEngine(strategy=DispersionStrategy())
results = engine.run(start_date='2020-01-01', end_date='2023-12-31')
```

2. Live Trading:

```python
from src.broker.ibkr_client import IBKRClient
from src.strategy.dispersion import DispersionStrategy

# Initialize IBKR connection and strategy
client = IBKRClient()
strategy = DispersionStrategy()

# Start live trading
client.connect()
strategy.run_live(client)
```

## Testing

Run tests using pytest:

```bash
pytest tests/
```

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

To use this implementation:

First, install the required dependencies:
`pip install -r requirements.txt`
Run
For backtesting:
`python examples/backtest_example.py`
Run
For live trading:
`python examples/live_trading_example.py`
Run
