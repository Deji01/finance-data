from glob import glob
import os
import pandas as pd
from model import CsvData
import psycopg2

def load_data():
    """
    Function to load data from a CSV file in the 'data' folder.
    Returns a pandas DataFrame object.
    """
    if os.path.exists("data") and any(file.endswith(".csv") for file in os.listdir("data")):
        print("CSV data found in the data folder")
        # read in data from path
        path = glob("data/*.csv")
    else:
        print("No CSV data found in the data folder")

    # load data from csv
    df = pd.read_csv(path[0])
    return df





def create_connection():
    """
    Function to create a connection to the PostgreSQL database.
    Returns a psycopg2 connection and cursor objects.
    """
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    cursor = conn.cursor()
    return conn, cursor

def create_table(conn, cursor):
    """
    Function to create a table in the PostgreSQL database.
    """
    create_table_query = """
    CREATE TABLE your_table_name (
        column1 TEXT NOT NULL,
        column2 INTEGER NOT NULL,
        column3 FLOAT NOT NULL
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

def insert_data(conn, cursor, data: pd.DataFrame):
    """
    Function to insert data from a pandas DataFrame into the PostgreSQL database.
    """
    insert_query = """
    INSERT INTO your_table_name (column1, column2, column3)
    VALUES (%s, %s, %s);
    """
    # insert data
    for row in data.itertuples(index=False):
        cursor.execute(insert_query, row)
    
    # commit changes
    conn.commit()

if  __name__ == "__main__":
    conn, cursor = create_connection()
    create_table(conn, cursor)
    data = load_data()
    insert_data(conn, cursor, data)
    cursor.close()
    conn.close()