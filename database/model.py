# create an SQLModel class that models the CSV data
from sqlmodel import SQLModel

class CsvData(SQLModel, table=True):
    column1: str
    column2: int
    column3: float