from helpers import create_node, create_node_relationships, format_date

from datetime import timedelta
import logging
from logging.handlers import RotatingFileHandler
import os

from dotenv import load_dotenv
from llama_index.core.settings import Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.timescalevector import TimescaleVectorStore
import pandas as pd
from sqlalchemy import create_engine, text


class YahooFinanceLoader:
    def __init__(self):
        """
        Initialize the class by loading environment variables, setting up API keys, initializing settings, and setting up logging.
        """
        # Load environment variables
        load_dotenv()

        self.OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
        self.HUGGINGFACEHUB_API_TOKEN = os.environ.get("HUGGINGFACEHUB_API_TOKEN")
        self.DB_URL = os.environ.get("DB_URL")
        self.VECTOR_STORE_URL = os.environ.get("VECTOR_STORE_URL")

        # Initialize settings
        Settings.llm = OpenAI(model="gpt-3.5-turbo-1106", temperature=0.1)
        Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """
        Set up logging with both stream handler and file handler at INFO level.
        """
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
            "logs/load_vector_store.log", maxBytes=1048576, backupCount=5
        )
        file_handler_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        log_file_handler.setFormatter(file_handler_formatter)
        self.logger.addHandler(log_file_handler)

    def get_data(self) -> pd.DataFrame:
        """
        Function to retrieve data from the database and return it as a pandas DataFrame.
        """
        self.logger.info("Starting to get data from the database")
        engine = create_engine(self.DB_URL)
        conn = engine.connect()
        self.logger.info("Database Connected")

        query = 'SELECT * FROM "yahoo-finance" WHERE DATE(created_at) = CURRENT_DATE;'

        try:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            conn.close()
            self.logger.info("Database connection closed")
            return df

        except Exception as e:
            conn.rollback()
            self.logger.error("Transaction rolled back due to exception: %s", e)
            conn.close()
            self.logger.info("Database connection closed")
            return pd.DataFrame()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the given DataFrame by dropping duplicates, filling NaN values with empty strings, and applying the format_date function to the "created_at" column. 
        Args:
            df (pd.DataFrame): The input DataFrame to be processed.
        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        df.drop_duplicates(inplace=True)
        df.fillna("", inplace=True)

        # Apply the format_date function to all rows in df["created_at"]
        df["formatted_created_at"] = df["created_at"].apply(format_date)
        return df

    def embed_nodes(self, nodes):
        """
        Embeds nodes using the specified embedding model.

        Args:
            nodes: A list of nodes to be embedded.

        Returns:
            None
        """
        embedding_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

        for node in nodes:
            node_embedding = embedding_model.get_text_embedding(
                node.get_content(metadata_mode="all")
            )
            node.embedding = node_embedding

    def store_vectors(self, nodes):
        """
        Store vectors in the TimescaleVectorStore and create an index.

        :param nodes: the vectors to be stored
        :return: None
        """
        ts_vector_store = TimescaleVectorStore.from_params(
            service_url=self.VECTOR_STORE_URL,
            table_name="yahoo-finance",
            num_dimensions=384,
            time_partition_interval=timedelta(days=1),
        )

        ts_vector_store.add(nodes)
        ts_vector_store.create_index()

    def run(self):
        """
        Run the function, get and process data, create nodes and relationships, embed nodes, store vectors, and log completion.
        """
        df = self.get_data()
        processed_df = self.process_data(df)
        text_nodes = [create_node(row) for _, row in processed_df.iterrows()]
        nodes = create_node_relationships(text_nodes)
        self.embed_nodes(nodes)
        self.store_vectors(nodes)
        self.logger.info("Loader run completed successfully")


if __name__ == "__main__":
    loader = YahooFinanceLoader()
    loader.run()
