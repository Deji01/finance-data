from glob import glob
import os
import pandas as pd
from model import CsvData, Config
from sqlmodel import SQLModel, Session, create_engine

import logging
from logging.handlers import RotatingFileHandler

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
        logger("CSV data found in the data folder")
        # read in data from path
        path = glob("data/*.csv")
    else:
        logger("No CSV data found in the data folder")

    # load data from csv
    df = pd.read_csv(path[0])
    return df


if __name__ == "__main__":
    csv_data = load_data()
    logger.info("CSV data loaded successfully.")

    # create an engine to connect to the PostgreSQL database
    engine = create_engine(Config().POSTGRES_URL)

    logger.info("Connected to the PostgreSQL database.")

    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # load data from pandas dataframe into the database
        for index, row in csv_data.iterrows():
            session.add(CsvData(**row.to_dict()))
            if index % 20 == 0:
                logger.info(f"Processed {index} rows.")
        logger.info("Data loaded successfully.")
        session.commit()
