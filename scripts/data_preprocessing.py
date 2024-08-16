import pandas as pd
import os
import logging
from multiprocessing import Pool, cpu_count

# Configure logging
logging.basicConfig(filename='data_preprocessing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_data(file_path):
    try:
        # Load data
        data = pd.read_csv(file_path)
        logging.info(f"Successfully loaded {file_path}")
        
        # Handle null values
        if data.isnull().sum().sum() > 0:
            logging.info(f"Handling missing values for {file_path}")
            data.fillna(method='ffill', inplace=True)  # Forward fill as an example
        
        # Convert Date column to datetime
        if 'Date' in data.columns:
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
            logging.info(f"Converted Date column to datetime for {file_path}")
        
        # Drop rows where Date conversion failed
        data = data.dropna(subset=['Date'])
        
        # Set Date as index
        data.set_index('Date', inplace=True)
        
        # Feature Engineering: Extracting Year, Month, Day from Date
        data['Year'] = data.index.year
        data['Month'] = data.index.month
        data['Day'] = data.index.day
        
        logging.info(f"Completed preprocessing for {file_path}")
        return data
    
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

def process_and_save(ticker):
    file_path = f"data/raw/{ticker}.csv"
    processed_data = preprocess_data(file_path)
    
    if processed_data is not None:
        output_file = f"data/processed/{ticker}_clean.csv"
        processed_data.to_csv(output_file)
        logging.info(f"Saved cleaned data for {ticker} to {output_file}")

if __name__ == "__main__":
    tickers = [
        "AAPL", "MSFT", "GOOGL", "BRK-B", "TSLA", "CVX", "NKE", "NVDA", "AMD", "ADBE", "QCOM", "META", "AMZN"
    ]
    
    # Ensure processed data directory exists
    os.makedirs('data/processed', exist_ok=True)
    
    # Utilize multiprocessing to process files in parallel
    with Pool(cpu_count()) as p:
        p.map(process_and_save, tickers)

    logging.info("Data preprocessing completed.")