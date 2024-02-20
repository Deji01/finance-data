from datetime import datetime
from glob import glob
import logging
from logging.handlers import RotatingFileHandler
import os
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests


class ArticleScraper:
    def __init__(self, base_path="data/*.csv"):
        self.base_path = base_path
        # Create logs directory if it does not exist
        if not os.path.exists("logs"):
            os.makedirs("logs")

        # Set up logging with both stream handler and file handler at INFO level
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Stream handler for console output
        stream_handler = logging.StreamHandler()
        stream_handler_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        stream_handler.setFormatter(stream_handler_formatter)
        self.logger.addHandler(stream_handler)

        # File handler for output to a log file with rotation
        log_file_handler = RotatingFileHandler(
            "logs/next.log", maxBytes=1048576, backupCount=5
        )
        file_handler_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        log_file_handler.setFormatter(file_handler_formatter)
        self.logger.addHandler(log_file_handler)

    def scrape_articles(self) -> pd.DataFrame:
        """
        Scrape articles from the base path and update the dataframe with the articles.

        Parameters:
        - self: the class instance

        Return:
        - df: the dataframe with articles
        """
        # read in data from path
        path = glob(self.base_path)
        if not path:
            self.logger.error("No CSV files found in the path.")
            return

        df = pd.read_csv(path[0])
        self.logger.info("CSV file loaded successfully.")

        # Update dataframe with articles
        df["article"] = df["url"].apply(self.get_article)
        return df

    def get_article(self, url: str) -> str:
        """
        Retrieves the content of an article from the specified URL.

        Parameters:
        url (str): The URL of the article.

        Returns:
        str: The content of the article.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            self.logger.info(f"Article retrieved successfully from {url}")
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return ""

        soup = BeautifulSoup(response.text, "html.parser")
        div_tag = soup.find("div", {"class": "caas-body"})
        if div_tag:
            article = div_tag.get_text().strip()
            article = re.sub(r"\s+", " ", article)
            return article
        else:
            self.logger.warning(f"No article content found for URL: {url}")
            return ""


if __name__ == "__main__":
    scraper = ArticleScraper()
    articles_df = scraper.scrape_articles()
    articles_df["article"] = articles_df["article"].fillna("")
    date = datetime.now().strftime("%Y-%m-%d")
    if articles_df is not None:
        articles_df.to_csv(f"data/scraped_articles_{date}.csv", index=False)
        scraper.logger.info("Scraped articles saved to scraped_articles.csv")
        # delete old files
        files = glob("data/*.csv")
        [
            os.remove(file)
            for file in files
            if file != f"data/scraped_articles_{date}.csv"
        ]
        scraper.logger.info(
            f"Deleted old CSV files except scraped_articles_{date}.csv file"
        )
