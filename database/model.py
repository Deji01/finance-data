# create an SQLModel class that models the CSV data
from sqlmodel import SQLModel, Field
from typing import Optional

class CsvData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str = Field(nullable=False)
    url: Optional[str] = None
    content: str
    article: str

