from sqlmodel import SQLModel, Field
from typing import Optional


class Article(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: Optional[str]
    title: Optional[str]
    url: Optional[str]
    content: Optional[str]
    article: Optional[str]