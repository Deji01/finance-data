from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
import re

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import date, timedelta
import pandas as pd

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

BASE_URL = "https://www.stocktitan.net"

async def fetch(url, session):
    """
    Asynchronously fetches data from the specified URL using the provided session.

    Args:
        url (str): The URL to fetch data from.
        session (aiohttp.ClientSession): The aiohttp client session to use for the request.

    Returns:
        str: The text content of the response.
    """
    async with session.get(url) as response:
        logger.info(f"Requesting {url}")
        return await response.text()


async def process_article(article_url, session, data):
    """
    Asynchronously processes an article given its URL, a session, and a data list.
    """
    html = await fetch(article_url, session)
    logger.info(f"Processing {article_url}")
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("div", class_="article")
    title = soup.find("h1")
    datetime = soup.find("time")
    impact_bar_container = soup.find("div", class_="impact-bar-container")
    impact = (
        impact_bar_container.find("span", class_="rhea-score")
        if impact_bar_container is not None
        else None
    )
    sentiment_bar_container = soup.find("div", class_="sentiment-bar-container")
    sentiment = (
        sentiment_bar_container.find("span", class_="rhea-score")
        if sentiment_bar_container is not None
        else None
    )
    news_card_summary = soup.find("div", class_="news-card-summary")
    summary = (
        news_card_summary.find("div", id="summary")
        if news_card_summary is not None
        else None
    )

    result = {
        "title": title.text.strip() if title is not None else "",
        "datetime": datetime.get("datetime") if datetime is not None else "",
        "impact_score": impact.text.strip().split("(")[-1].split(")")[0]
        if impact is not None
        else "",
        "sentiment": sentiment.text.strip().split("(")[-1].split(")")[0]
        if sentiment is not None
        else "",
        "summary": summary.get_text().strip() if summary is not None else "",
        "article": re.sub("\s+", " ", article.get_text()).strip()
        if article is not None
        else "",
    }

    data.append(result)


async def process_date(date_str, session, data):
    """
    Asynchronously processes the given date string to fetch news articles from the stocktitan website. 
    Parameters:
        date_str (str): The date string used to construct the URL for fetching news articles.
        session (aiohttp.ClientSession): The aiohttp client session used for making HTTP requests.
        data (dict): A dictionary containing additional data to be processed along with the news articles.

    Returns:
        None
    """
    url = f"https://www.stocktitan.net/news/{date_str}/"
    html = await fetch(url, session)
    soup = BeautifulSoup(html, "html.parser")
    news_rows = soup.find_all("div", class_="news-row")
    all_links = [
        BASE_URL + row.find("a", class_="feed-link").get("href") for row in news_rows
    ]
    logger.info(f"Found {len(all_links)} news articles for {date_str}.")
    tasks = []
    for article_url in all_links:
        tasks.append(process_article(article_url, session, data))
    logger.info(f"Processed {len(tasks)} news articles for {date_str}.")
    await asyncio.gather(*tasks)


async def main():
    """
    A description of the entire function, its parameters, and its return types.
    """
    async with aiohttp.ClientSession() as session:
        data = []
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(
                process_date(
                    str(date.fromisoformat(str(date.today())) - timedelta(days=i)),
                    session,
                    data,
                )
            )
            for i in range(4)
        ]
        await asyncio.gather(*tasks)

        df = pd.DataFrame(data)
        logger.info(f"Dataframe has {len(df)} rows before cleaning up")
        # clean up dataframe by removing empty rows
        data = df.copy()
        data.drop_duplicates(inplace=True)
        logger.info(f"Dataframe had {df.shape[0]} rows before removing empty rows")
        logger.info(f"Dataframe has {data.shape[0]} rows after removing empty rows")
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = f"data/stocktitan_{date_str}.csv"
        data.to_csv(path, index=False)
        logger.info(f"Data saved to {path}")


if __name__ == "__main__":
    asyncio.run(main())
