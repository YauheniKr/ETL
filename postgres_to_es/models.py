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


@dataclass(frozen=True)
class BaseTableClass:
    table_list: List[str]

    @property
    def table_to_check_update(self):
        return self.table_list[0]


@dataclass(frozen=True)
class PersonTables(BaseTableClass):
    pass


@dataclass(frozen=True)
class GenreTables(BaseTableClass):
    pass


@dataclass(frozen=True)
class FilmWorkTables(BaseTableClass):
    pass
