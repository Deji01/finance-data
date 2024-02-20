from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

from bs4 import BeautifulSoup
import pandas as pd
import requests


class NewsScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url
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
            "logs/main.log", maxBytes=1048576, backupCount=5
        )
        file_handler_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        log_file_handler.setFormatter(file_handler_formatter)
        self.logger.addHandler(log_file_handler)

    def get_page(self, url: str) -> BeautifulSoup:
        """
        Get the webpage content from the specified URL and return it as a BeautifulSoup object.

        Args:
            url (str): The URL of the webpage to download.

        Returns:
            BeautifulSoup: The parsed HTML content of the webpage.
        """
        self.logger.info(f"Downloading webpage: {url}")
        # make a request to the url and wait for page to load completely
        response = requests.get(url, timeout=30)

        if not response.ok:
            self.logger.error(
                f"Failed to load {url} with status code {response.status_code}\n"
            )
            raise Exception(
                f"Failed to load {url} with status code {response.status_code}\n"
            )

        soup = BeautifulSoup(response.text, "html.parser")
        return soup

    def get_news_tags(self, soup: BeautifulSoup) -> list:
        """
        Get news tags from the BeautifulSoup object.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object containing the parsed HTML.

        Returns:
            list: A list of news tags extracted from the page.
        """
        self.logger.info("Extracting news tags from page")

        return soup.find_all("div", {"class": "Ov(h) Pend(44px) Pstart(25px)"})

    def parse_news(self, div_tag) -> dict:
        """
        Parse news from a div tag and return a dictionary containing the source, title, URL, and content.

        Parameters:
            div_tag: The div tag containing the news information.

        Returns:
            A dictionary with the keys "source", "title", "url", and "content" containing the parsed news information.
        """
        source = div_tag.find("div").text
        title = div_tag.find("a").text
        url = self.base_url + div_tag.find("a")["href"]
        content = div_tag.find("p").text

        return {"source": source, "title": title, "url": url, "content": content}

    def scrape_news(self, path: str = None) -> pd.DataFrame:
        """
        Scrapes news data from a website and saves it to a CSV file. If no path is provided, a default filename based on the current date is used. Returns a pandas DataFrame containing the scraped news data.

        Parameters:
            path (str, optional): The path to save the CSV file. Defaults to None.

        Returns:
            pandas.DataFrame: The DataFrame containing the scraped news data.
        """
        if path is None:
            date = datetime.now().strftime("%Y-%m-%d")
            path = f"data/stock_market_news_{date}.csv"

        full_url = self.base_url + "/topic/stock-market-news/"

        self.logger.info(f"Starting news scraping for URL: {full_url}")

        doc = self.get_page(url=full_url)
        div_tags = self.get_news_tags(soup=doc)

        news = [self.parse_news(div) for div in div_tags]
        self.logger.info(f"{len(news)} news extracted")

        df = pd.DataFrame(news)
        df.to_csv(path, index=False)

        self.logger.info(f"News data saved to {path}")

        return df


if __name__ == "__main__":
    scraper = NewsScraper(base_url="https://finance.yahoo.com")
    news_df = scraper.scrape_news()
