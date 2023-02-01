from dataclasses import dataclass, field
import uuid


@dataclass
class FilmWork:
    title: str
    description: str
    type: str
    created: str
    creation_date: str = field(default="1900-01-01 12:00:00.000000+00")
    rating: float = field(default=0.0)
    modified: str = field(default="")
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    description: str
    created: str
    modified: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created: str
    modified: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: str = field(default="")
    genre_id: str = field(default="")
    created: str = field(default="")


@dataclass
class PersonFilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: str = field(default="")
    person_id: str = field(default="")
    role: str = field(default="")
    created: str = field(default="")


if __name__ == '__main__':
    pass