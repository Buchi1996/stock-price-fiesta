import yfinance as yf
import os

def download_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    return data

if __name__ == "__main__":
    tickers = [
        "AAPL", "MSFT", "GOOGL", "BRK-B", "TSLA", "CVX", "NKE", "NVDA", "AMD", "ADBE", "QCOM", "META", "AMZN"
    ]
    start_date = "2008-06-03"
    end_date = "2024-06-15"
    
    # Ensure raw data directory exists
    os.makedirs('data/raw', exist_ok=True)
    
    for ticker in tickers:
        data = download_data(ticker, start_date, end_date)
        data.to_csv(f"data/raw/{ticker}.csv")