import random

import requests
import typer
import faker
from pydantic import BaseModel

fake = faker.Faker()
app = typer.Typer()


class User(BaseModel):
    email: str
    age: int


class Post(BaseModel):
    body: str


def generate_user() -> User:
    return User(email=fake.ascii_email(), age=random.randint(14, 76))


def generate_post() -> Post:
    return Post(body=fake.paragraph(nb_sentences=random.randint(1, 6)))


def post_user(user: User) -> int:
    response = requests.post("http://localhost:8000/users", json=user.dict())
    return response.json()["id"]


def post_post(user_id: int, post: Post) -> int:
    response = requests.post(f"http://localhost:8000/users/{user_id}/posts/", json=post.dict())
    return response.json()["id"]


@app.command()
def populate_posts(user_count: int = 30, min_posts_per_user: int = 5, max_posts_per_user: int = 30) -> None:
    user_ids = [post_user(generate_user()) for _ in range(user_count)]
    for user_id in user_ids:
        for _ in range(random.randint(min_posts_per_user, max_posts_per_user)):
            post_post(user_id, generate_post())


if __name__ == '__main__':
    app()
