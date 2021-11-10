import uuid
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Person:
    id: uuid
    name: str


@dataclass(frozen=True)
class Filmwork:
    id: uuid
    title: str
    rating: float
    description: str
    writers: List[Person]
    actors: List[Person]
    writers_names: List[str]
    actors_names: List[str]
    director: List[str]
    genre: List[str]
