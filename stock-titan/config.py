from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_PORT: str
    DB_URL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Instantiate settings
settings = Settings()
