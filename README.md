# bnc-markets
Script for gathering historical marketdata from Binance without using the Binance API

# Installation
```console
clone https://github.com/TobyWanKanobi/bnc-markets.git
cd bnc-markets
pip install
```
Copy ".env-example" and rename the copy to ".env"

# Usage

Gather all trades from Binance for "DOGEUSDT" from 1-1-2023 to 12-4-2023:
```console
python bnc_markets.py --symbol DOGEUSDT --start 01-01-2023 --end 12-04-2023
```

# Requirements
Python 3.9.0

<img src="https://www.python.org/static/img/python-logo.png" style="padding-top: -100px; width:30%; height:30%;" alt="Employee data" title="Employee Data title" />
<img src="https://download.logo.wine/logo/Binance/Binance-Logo.wine.png" style="width:30%; height:30%;" alt="Employee data" title="Employee Data title" />
