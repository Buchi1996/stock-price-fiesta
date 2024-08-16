# stock-price-fiesta 
A project to forecast stock prices and identify market regimes using advanced machine learning techniques. Basically, a Stock Price Forecasting and Market Regime Identification project

## Table of Contents

- [Introduction](#introduction)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Introduction

Stock price forecasting is a critical task in financial analysis, enabling investors and traders to make informed decisions. This project explores the application of various machine learning models, including Long Short-Term Memory (LSTM), Autoregressive Integrated Moving Average (ARIMA), and Prophet, to predict stock prices based on historical data. Additionally, it includes an algorithm to classify market regimes.

## Project Structure

Here's a brief overview of the project's structure:

```plaintext
stock-price-forecasting/
├── data/                   # Contains raw and processed datasets
│   ├── raw/                # Raw data files
│   └── processed/          # Processed data files
├── scripts/                # Python scripts for various tasks
│   ├── data_collection.py  # Script to download stock data
│   ├── data_preprocessing.py # Script to preprocess data
│   ├── eda.py              # Script for exploratory data analysis
│   ├── lstm_model.py       # Script to train and evaluate LSTM model
│   ├── arima_model.py      # Script to train and evaluate ARIMA model
│   ├── prophet_model.py    # Script to train and evaluate Prophet model
│   └── market_regime.py    # Script to identify market regimes
├── notebooks/              # Jupyter Notebooks for analysis
│   ├── eda.ipynb           # Notebook for exploratory data analysis
│   ├── modeling_lstm.ipynb # Notebook for LSTM model development
│   ├── modeling_arima.ipynb # Notebook for ARIMA model development
│   ├── modeling_prophet.ipynb # Notebook for Prophet model development
│   └── market_regime_analysis.ipynb # Notebook for market regime analysis
├── docs/                   # Documentation files
│   ├── project_overview.md # Overview of the project
│   ├── setup_instructions.md # Setup instructions
│   └── model_descriptions.md # Descriptions of models used
├── README.md               # Project README file
├── requirements.txt        # List of dependencies
└── .gitignore              # Git ignore file


Usage
Data Collection
Run the data collection script to download stock data:

python scripts/data_collection.py
Data Preprocessing
Run the data preprocessing script to clean and preprocess the data:

python scripts/data_preprocessing.py
Exploratory Data Analysis
Open the notebooks/eda.ipynb notebook to explore and visualize the data.

Model Training and Evaluation
Run the respective scripts to train and evaluate the models:

python scripts/lstm_model.py
python scripts/arima_model.py
python scripts/prophet_model.py
Market Regime Identification
Run the market regime identification script:

python scripts/market_regime.py
