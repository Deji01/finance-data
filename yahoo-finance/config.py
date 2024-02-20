from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SUPABASE_REST_ENDPOINT: str
    SUPABASE_CLIENT_ANON_KEY: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Instantiate settings
settings = Settings()