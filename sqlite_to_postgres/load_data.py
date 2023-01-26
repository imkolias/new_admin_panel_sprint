import sqlite3
import psycopg2
import os
import uuid
from itertools import islice
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
from dataclasses import dataclass, field


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
    file_path: str
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


def sqliterator(cursor, arraysize=1000):
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for item in results:
            yield item


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
        self.curs.execute(f"SELECT COUNT(id) FROM content.{table_name};")
        saved_count = (self.curs.fetchone())
        if count == int(saved_count[0]):
            print('OK, saved:', saved_count[0])
        else:
            print('ERROR, wrong save count, should be:', count,
                  'saved:', saved_count[0])
        print()

    def save_movies(self, mdata: list):
        print('[Save Movies table in db]')
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print('Elements to save:', elems_count)
        for time in range(times + 1):
            print('.', end='')
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.title, elem.description,
                                   elem.rating, elem.type, elem.created_at,
                                   elem.updated_at))

            self.curs.executemany(
                "INSERT INTO content.film_work (id, title, description, "
                "creation_date, rating, type, created, modified) VALUES "
                "(%s, %s, %s, current_timestamp, %s, %s, %s, %s) ON CONFLICT"
                " (id) DO UPDATE SET id=EXCLUDED.id;", sql_params)

        self.save_check(elems_count, 'film_work')

    def save_persons(self, mdata: list):
        print('[Save Person table in db]')
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print('Elements to save:', elems_count)
        for time in range(times + 1):
            print('.', end='')
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.full_name,
                                   elem.created_at, elem.updated_at))

            self.curs.executemany(
                "INSERT INTO content.person (id, full_name, created, modified)"
                " VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO UPDATE "
                "SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, 'person')

    def save_genres(self, mdata: list):
        print('[Save Genres table in db]')
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        print('Elements to save:', elems_count)
        for time in range(times + 1):
            print('.', end='')
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.name,
                                   elem.created_at, elem.updated_at))

            self.curs.executemany(
                "INSERT INTO content.genre (id, name, created, modified) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (id) "
                "DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, 'genre')

    def save_genre_film_work(self, mdata: list):
        print('[Save Genres Film Work table in db]')
        elems_count = len(mdata)
        print('Elements to save:', elems_count)
        times = elems_count // self.slicesize
        for time in range(times + 1):
            print('.', end='')
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.film_work_id,
                                   elem.genre_id, elem.created_at))

            self.curs.executemany(
                "INSERT INTO content.genre_film_work (id, film_work_id, "
                "genre_id, created) VALUES (%s, %s, %s, %s) ON CONFLICT (id)"
                " DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, 'genre_film_work')

    def save_person_film_work(self, mdata: list):
        print('[Save Persons Film Work table in db]')
        elems_count = len(mdata)

        times = elems_count // self.slicesize
        for time in range(times + 1):
            print('.', end='')
            sql_params = []
            for elem in islice(mdata, 0 + (time * 100), 100 + (time * 100)):
                sql_params.append((elem.id, elem.film_work_id, elem.person_id,
                                   elem.role, elem.created_at))

            self.curs.executemany(
                "INSERT INTO content.person_film_work (id, film_work_id,"
                " person_id, role, created) VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id;", sql_params)
        self.save_check(elems_count, 'person_film_work')


class SQLiteExtractor:
    curs = 0

    def __init__(self, connection: sqlite3.Connection):
        self.curs = connection.cursor()
        print('[Start read data from SQLite db]')

    def extract_movies(self):
        self.curs.execute("SELECT * FROM film_work;")
        print('Get data from film_work')
        return [Movie(**elem) for elem in sqliterator(self.curs)]

    def extract_persons(self):
        self.curs.execute("SELECT * FROM person;")
        print('Get data from person')
        return [Person(**elem) for elem in sqliterator(self.curs)]

    def extract_genres(self):
        self.curs.execute("SELECT * FROM genre;")
        print('Get data from genre')
        return [Genre(**elem) for elem in sqliterator(self.curs)]

    def extract_genresfilmwork(self):
        self.curs.execute("SELECT * FROM genre_film_work;")
        print('Get data from genre_film_work')
        return [GenreFilmWork(**elem) for elem in sqliterator(self.curs)]

    def extract_person_film_work(self):
        self.curs.execute("SELECT * FROM person_film_work;")
        print('Get data from person_film_work')
        return [PersonFilmWork(**elem) for elem in sqliterator(self.curs)]


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = DataContainer(sqlite_extractor.extract_movies(),
                         sqlite_extractor.extract_genres(),
                         sqlite_extractor.extract_persons(),
                         sqlite_extractor.extract_genresfilmwork(),
                         sqlite_extractor.extract_person_film_work(),
                         )

    print()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    sqlitedbfile = 'db.sqlite'

    if os.path.exists(sqlitedbfile):

        try:
            with conn_context(sqlitedbfile) as sqlite_conn, \
                    psycopg2.connect(**dsl, cursor_factory=DictCursor) as pgc:
                load_from_sqlite(sqlite_conn, pgc)
        except psycopg2.OperationalError:
            print('[Error] - Can\'t connect to Postgres DB')

    else:
        print('[Error] - Can\'t find source Sqlite3 DB file:', sqlitedbfile)
