from sqlmodel import SQLModel, Field, DateTime
from typing import Optional

class Article(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    title: Optional[str] = None
    datetime: Optional[DateTime] = None
    impact_score: Optional[str] = None
    sentiment: Optional[str] = None
    summary: Optional[str] = None
    article: Optional[str] = None
