from dataclasses import dataclass, field
import uuid

@dataclass
class Movie:
    type: str
    title: str
    description: str
    creation_date: str
    # file_path: str
    created_at: str
    updated_at: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    created_at: str
    updated_at: str
    description: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created_at: str
    updated_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    film_work_id: str
    genre_id: str
    created_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    film_work_id: str
    person_id: str
    role: str
    created_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class DataContainer:
    movies: list
    genres: list
    persons: list
    genre_film_works: list
    person_film_works: list





if __name__ == '__main__':
    print("[Warning] It's module file, not for run!")
