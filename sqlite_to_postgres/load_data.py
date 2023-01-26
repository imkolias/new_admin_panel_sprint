import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import os
from dotenv import load_dotenv

from contextlib import contextmanager

from dataclasses import dataclass, field
import uuid
from itertools import islice

load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content',
}


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@dataclass
class Movie:
    type: str
    title: str
    description: str
    creation_date: str
    created_at: str
    updated_at: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    created_at: str
    updated_at: str
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


class PostgresSaver:

    def __init__(self, pgcon: _connection):
        self.curs = pgcon.cursor()
        self.slicesize = 100

    def save_all_data(self, data: DataContainer):
        self.data = data
        self.save_movies(self.data.movies)
        self.save_persons(self.data.persons)
        self.save_genres(self.data.genres)
        self.save_genre_film_work(self.data.genre_film_works)
        self.save_person_film_work(self.data.person_film_works)

    def save_check(self, count: int, table_name: str):
        self.curs.execute(f"SELECT COUNT(id) FROM {table_name}")
        saved_count = (self.curs.fetchone())
        if count == int(saved_count[0]):
            print("OK, saved:", saved_count[0])
        else:
            print("ERROR, wrong save count, should be:", count, "saved:", saved_count[0])
        print()

    def save_movies(self, mdata: list):
        print("[Save Movies table in db]")
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print("Elements to save:", elems_count)
        for time in range(times + 1):
            print(".", end="")
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.title, elem.description, elem.rating, elem.type, elem.created_at,
                                   elem.updated_at))

            self.curs.executemany("INSERT INTO content.film_work (id, title, description, creation_date, rating, "
                                  "type, created, modified) VALUES (%s, %s, %s, current_timestamp, %s, %s, %s, %s)"
                                  " ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, "content.film_work")

    def save_persons(self, mdata: list):
        print("[Save Person table in db]")
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print("Elements to save:", elems_count)
        for time in range(times + 1):
            print(".", end="")
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.full_name, elem.created_at, elem.updated_at))

            self.curs.executemany("INSERT INTO content.person (id, full_name, created, modified)"
                                  " VALUES (%s, %s, %s, %s) ON CONFLICT "
                                  "(id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, "content.person")

    def save_genres(self, mdata: list):
        print("[Save Genres table in db]")
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print("Elements to save:", elems_count)
        for time in range(times + 1):
            print(".", end="")
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.name, elem.created_at, elem.updated_at))

            self.curs.executemany("INSERT INTO content.genre (id, name, created, modified)"
                                  " VALUES (%s, %s, %s, %s) ON CONFLICT "
                                  "(id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, "content.genre")

    def save_genre_film_work(self, mdata: list):
        print("[Save Genres Film Work table in db]")
        elems_count = len(mdata)
        print("Elements to save:", elems_count)
        times = elems_count // self.slicesize
        print(".", end="")
        for time in range(times + 1):
            print(".", end="")
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.film_work_id, elem.genre_id, elem.created_at))

            self.curs.executemany("INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created)"
                                  " VALUES (%s, %s, %s, %s) ON CONFLICT "
                                  "(id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, "content.genre_film_work")

    def save_person_film_work(self, mdata: list):
        print("[Save Persons Film Work table in db]")
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print(".", end="")
        for time in range(times + 1):
            print(".", end="")
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.film_work_id, elem.person_id, elem.role, elem.created_at))

            self.curs.executemany("INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created)"
                                  " VALUES (%s, %s, %s, %s, %s) ON CONFLICT "
                                  "(id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, "content.person_film_work")


class SQLiteExtractor:
    curs = 0

    def __init__(self, connection: sqlite3.Connection):
        self.curs = connection.cursor()

    def extract_movies(self):
        self.curs.execute(
            "SELECT id, title, description, creation_date, rating, type, created_at, updated_at FROM film_work;")
        rows = self.curs.fetchall()

        return [Movie(**row) for row in rows]

    def extract_persons(self):
        self.curs.execute("SELECT id, full_name, created_at, updated_at FROM person;")
        rows = self.curs.fetchall()

        return [Person(**row) for row in rows]

    def extract_genres(self):
        self.curs.execute("SELECT id, name, created_at, updated_at FROM genre;")
        rows = self.curs.fetchall()

        return [Genre(**row) for row in rows]

    def extract_genresfilmwork(self):
        self.curs.execute("SELECT id, film_work_id, genre_id, created_at FROM genre_film_work;")
        rows = self.curs.fetchall()

        return [GenreFilmWork(**row) for row in rows]

    def extract_person_film_work(self):
        self.curs.execute("SELECT id, film_work_id, person_id, role, created_at FROM person_film_work;")
        rows = self.curs.fetchall()

        return [PersonFilmWork(**row) for row in rows]


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = DataContainer(sqlite_extractor.extract_movies(),
                         sqlite_extractor.extract_genres(),
                         sqlite_extractor.extract_persons(),
                         sqlite_extractor.extract_genresfilmwork(),
                         sqlite_extractor.extract_person_film_work()
                         )

    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    sqlitedbfile = 'db.sqlite'

    if os.path.exists(sqlitedbfile):

        try:
            with conn_context(sqlitedbfile) as sqlite_conn, psycopg2.connect(**dsl,
                                                                             cursor_factory=DictCursor) as pg_conn:
                load_from_sqlite(sqlite_conn, pg_conn)
        except psycopg2.OperationalError:
            print("[Error] - Can't connect to Postgres DB")

    else:
        print("[Error] - Can't find source Sqlite3 DB file:", sqlitedbfile)
