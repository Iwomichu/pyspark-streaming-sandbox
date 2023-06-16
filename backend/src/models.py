import os
from datetime import datetime
from typing import Optional, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlmodel import SQLModel, Field, Relationship

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE", "postgres")
engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}")
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class UserBase(SQLModel):
    email: str
    age: int


class User(UserBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    posts: List["Post"] = Relationship(back_populates="poster")


class PostBase(SQLModel):
    body: str


class Post(PostBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(default_factory=datetime.now)
    poster_id: int = Field(foreign_key="user.id")

    poster: User = Relationship(back_populates="posts")
