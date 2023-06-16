from datetime import datetime
from typing import List

from pydantic import BaseModel


class PostBase(BaseModel):
    body: str


class PostCreate(PostBase):
    pass


class Post(PostBase):
    id: int
    poster_id: int
    timestamp: datetime

    class Config:
        orm_mode = True


class UserBase(BaseModel):
    email: str
    age: int


class UserCreate(UserBase):
    pass


class User(UserBase):
    id: int
    posts: List[Post] = []

    class Config:
        orm_mode = True
