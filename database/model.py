from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import BaseModel, Field
class Settings(BaseSettings):
    DB_URL: str
    SUPABASE_CLIENT_ANON_KEY: str
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Instantiate settings
settings = Settings()

class CsvData(BaseModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: Optional[str] = None
    url: Optional[str] = None
    content: Optional[str] = None
    article: Optional[str] = None
