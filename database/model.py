# create an SQLModel class that models the CSV data
from sqlmodel import SQLModel, Field
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_URL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Instantiate settings
settings = Settings()

class CsvData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: Optional[str] = None
    url: Optional[str] = None
    content: Optional[str] = None
    article: Optional[str] = None
