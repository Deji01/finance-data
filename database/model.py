# create an SQLModel class that models the CSV data
from sqlmodel import SQLModel, Field
from typing import Optional
from pydantic_settings import Settings


class Config(Settings):
    POSTGRES_URL: str
    env_file = ".env"


class CsvData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: Optional[str] = None
    url: Optional[str] = None
    content: Optional[str] = None
    article: Optional[str] = None
