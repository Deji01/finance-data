from config import settings

from glob import glob
import logging
from logging.handlers import RotatingFileHandler
import os

import pandas as pd
from sqlalchemy import create_engine

# Create logs directory if it does not exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Set up logging with both stream handler and file handler at INFO level
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
stream_handler.setFormatter(stream_handler_formatter)
logger.addHandler(stream_handler)

# File handler for output to a log file with rotation
log_file_handler = RotatingFileHandler("logs/main.log", maxBytes=1048576, backupCount=5)
file_handler_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log_file_handler.setFormatter(file_handler_formatter)
logger.addHandler(log_file_handler)


def load_data():
    """
    Function to load data from a CSV file in the 'data' folder.
    Returns a pandas DataFrame object.
    """
    if os.path.exists("data") and any(
        file.endswith(".csv") for file in os.listdir("data")
    ):
        logger.info("CSV data found in the data folder")
        # read in data from path
        path = glob("data/*.csv")
    else:
        logger.info("No CSV data found in the data folder")

    # load data from all csv files
    df = pd.concat([pd.read_csv(file) for file in path], ignore_index=True)
    df.drop_duplicates(inplace=True)
    return df


def create_archive():
    """
    Function to create an archive of the data in the 'data' folder.
    """
    if not os.path.exists("archive"):
        # create archive folder
        os.makedirs("archive")
        logger.info("Archive folder created.")

    # move data to archive folder
    for file in os.listdir("data"):
        os.rename(f"data/{file}", f"archive/{file}")
        logger.info(f"{file} moved to archive folder.")


if __name__ == "__main__":
    df = load_data()
    logger.info("CSV data loaded successfully.")

    # create an engine to connect to a database
    engine = create_engine(settings.DB_URL)
    conn = engine.connect()
    logger.info("Connected to the database.")

    df.to_sql("yahoo-finance", con=conn, if_exists="append", index=False)

    conn.autocommit = True
    logger.info("Data loaded successfully.")
    conn.close()
    logger.info("Connection closed.")
    create_archive()
    logger.info("Archive created.")
