import logging
from logging.handlers import RotatingFileHandler
import os

import pandas as pd
import requests

# Create logs directory if it does not exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging with both stream handler and file handler at INFO level
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(stream_handler_formatter)
logger.addHandler(stream_handler)

# File handler for output to a log file with rotation
log_file_handler = RotatingFileHandler('logs/main.log', maxBytes=1048576, backupCount=5)
file_handler_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_file_handler.setFormatter(file_handler_formatter)
logger.addHandler(log_file_handler)

def stock_tickers()-> pd.DataFrame:
    """
    Function to retrieve stock tickers as a pandas DataFrame.
    """
    url = "https://supabase-websocket.dev.marketreader.com/symbols"
    headers = {
        "authority": "supabase-websocket.dev.marketreader.com",
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "origin": "https://app.marketreader.com",
        "pragma": "no-cache",
        "referer": "https://app.marketreader.com/",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json())

def get_stock_data(ticker: str)-> pd.DataFrame:
    """
    Retrieves stock data for the given ticker symbol in the form of a pandas DataFrame.

    Args:
        ticker (str): The ticker symbol of the stock to retrieve data for.

    Returns:
        pd.DataFrame: The stock data in the form of a pandas DataFrame.
    """

    url = f"https://supabase-websocket.dev.marketreader.com/outputs/longterm/{ticker.upper()}/5W"
    headers = {
        "authority": "supabase-websocket.dev.marketreader.com",
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzA4MzM4NTUwLCJzdWIiOiJjYzNhZWQzMC05NDAwLTQxY2MtYjE5OC03MTUzNzMzNTA5YTUiLCJlbWFpbCI6Inlla2VlbnJ5YmFja0BnbWFpbC5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7fSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJwYXNzd29yZCIsInRpbWVzdGFtcCI6MTcwODI1MjE1MH1dLCJzZXNzaW9uX2lkIjoiNWRhNmE2ZmQtYTg4NS00MjU5LTgzOTYtZjFkMzI0ZjJmMTkxIn0.6u5kWP3-735SLx2AaQhVo2X3L4YvSBLPsoe8qb3BGUI",
        "cache-control": "no-cache",
        "origin": "https://app.marketreader.com",
        "pragma": "no-cache",
        "referer": "https://app.marketreader.com/",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json())

if __name__ == "__main__":
    logger.info("Starting market reader")
    tickers = stock_tickers()["symbol"].tolist()
    logger.info(f"Retrieving stock data for {len(tickers)} tickers")
    # create empty dataframe
    df = pd.DataFrame()
    # get stock data for each ticker
    for i, ticker in enumerate(tickers):
        df = pd.concat([df, get_stock_data(ticker)])
        # save dataframe to csv file every 25 tickers
        if i % 25 == 0:
            logger.info(f"Retrieved data for {i} tickers")  
            df.to_csv("data/stock_data.csv", index=False)
    # save dataframe to csv file
    logger.info("Saving stock data to csv file")
    df.to_csv("data/stock_data.csv", index=False)
    logger.info("Market reader finished")