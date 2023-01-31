from dataclasses import dataclass, field
import uuid


@dataclass
class FilmWork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = field(default="")
    description: str = field(default="")
    creation_date: str = field(default="")
    rating: float = field(default=0.0)
    type: str = field(default="")
    created: str = field(default="")
    modified: str = field(default="")


@dataclass
class Genre:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = field(default="")
    description: str = field(default="")
    created: str = field(default="")
    modified: str = field(default="")


@dataclass
class Person:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str = field(default="")
    created: str = field(default="")
    modified: str = field(default="")


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
    print("[Warning] It's module file, not for run!")
