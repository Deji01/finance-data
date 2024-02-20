from config import settings

from glob import glob
import logging
from logging.handlers import RotatingFileHandler
import os

import pandas as pd
from rich import print


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

    # load data from csv
    df = pd.read_csv(path[0])
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


def execute_curl_command(command):
    """
    Function to execute a curl command and return the output.
    """
    import subprocess

    output = subprocess.check_output(command, shell=True)
    return output


if __name__ == "__main__":
    df = load_data()

    data = df.to_dict(orient="records")

    # send data to Supabase REST API
    for row in data:
        command = f"""
        curl -X POST '{settings.SUPABASE_REST_ENDPOINT}/yahoo-finance' \
        -H "apikey: {settings.SUPABASE_CLIENT_ANON_KEY}" \
        -H "Authorization: Bearer {settings.SUPABASE_CLIENT_ANON_KEY}" \
        -H "Content-Type: application/json" \
        -d '{[row]}'
        """
        output = execute_curl_command(command)
        print(output)

    # create_archive()
