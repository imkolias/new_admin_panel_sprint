import sqlite3
import psycopg2
import os
import unittest
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
from dataclasses import dataclass, field
import uuid

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
class SQMovie:
    type: str
    title: str
    description: str
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


table_list = ['film_work', 'person', 'genre',
              'genre_film_work', 'person_film_work']


class TestDB(unittest.TestCase):

    def test1_connection(self):
        self.assertEqual(sqlite_db_check(), True,
                         "No connection to one of SQLite db")
        self.assertEqual(postgres_db_check(), True,
                         "No connection to one of Postgres db")

    def test2_equality_test(self):
        sqlitedbfile = '..\\..\\db.sqlite'

        with conn_context(sqlitedbfile) as sqlite_conn, \
                psycopg2.connect(**dsl, cursor_factory=DictCursor) as pgc:
            sqlite_cur = sqlite_conn.cursor()
            posg_cur = pgc.cursor()

            for table in table_list:
                self.assertEqual(t_equality_test(sqlite_cur, table),
                                 t_equality_test(posg_cur, table),
                                 f'error {table} tables not equal')

    def test3_data_integrity_test(self):
        sqlitedbfile = '..\\..\\db.sqlite'

        with conn_context(sqlitedbfile) as sqlite_conn, \
                psycopg2.connect(**dsl, cursor_factory=DictCursor) as pgc:
            sqlite_cur = sqlite_conn.cursor()
            posg_cur = pgc.cursor()

            self.assertEqual(
                row_equality(sqlite_cur, posg_cur, 'film_work', 999), True,
                'error "film_work" tables not equal')

            self.assertEqual(
                row_equality(sqlite_cur, posg_cur, 'genre', 100), True,
                'error "genre" tables not equal')

            self.assertEqual(
                row_equality(sqlite_cur, posg_cur, 'person', 5000), True,
                'error "person" tables not equal')

            self.assertEqual(
                row_equality(sqlite_cur, posg_cur, 'genre_film_work', 2500),
                True, 'error "genre_film_work" tables not equal')

            self.assertEqual(
                row_equality(sqlite_cur, posg_cur, 'person_film_work', 5600),
                True, 'error "person_film_work" tables not equal')


def t_equality_test(cursor, table_name: str):
    squery = f"SELECT count(id) FROM {table_name};"
    cursor.execute(squery)
    return cursor.fetchone()[0]


def row_equality(sqlite_cur, posg_cur, table_name: str, row_limit: int):
    if table_name == "film_work":
        # for table in table_list:
        squery = f"SELECT id, title, description, rating, type," \
                 f" created_at, updated_at FROM {table_name} " \
                 f"ORDER BY ID LIMIT {row_limit};"
        sqlite_cur.execute(squery)
        sqlite_datalist = [SQMovie(**item) for item in sqlite_cur.fetchall()]

        squery = f"SELECT id, title, description, rating, type, created" \
                 f" as created_at, modified  as updated_at FROM " \
                 f"{table_name} ORDER BY ID LIMIT {row_limit};"
        posg_cur.execute(squery)
        pg_datalist = [SQMovie(**item) for item in posg_cur.fetchall()]

        result_flag = False
        for n in range(len(sqlite_datalist)):
            if (sqlite_datalist[n].title == pg_datalist[n].title and
                    sqlite_datalist[n].description ==
                    pg_datalist[n].description and
                    sqlite_datalist[n].id == pg_datalist[n].id and
                    sqlite_datalist[n].rating == pg_datalist[n].rating and
                    sqlite_datalist[n].type == pg_datalist[n].type and
                    str(sqlite_datalist[n].created_at)[0:19] ==
                    str(pg_datalist[n].created_at)[0:19] and
                    str(sqlite_datalist[n].updated_at)[0:19] ==
                    str(pg_datalist[n].updated_at)[0:19]):
                result_flag = True
            else:
                result_flag = False

    elif table_name == "genre":
        squery = f"SELECT id, name, description, created_at , updated_at " \
                 f"FROM {table_name} ORDER BY ID LIMIT {row_limit};"
        sqlite_cur.execute(squery)
        sqlite_datalist = [Genre(**item) for item in sqlite_cur.fetchall()]

        squery = f"SELECT id, name, description, created as created_at," \
                 f" modified  as updated_at FROM {table_name} " \
                 f"ORDER BY ID LIMIT {row_limit};"
        posg_cur.execute(squery)
        pg_datalist = [Genre(**item) for item in posg_cur.fetchall()]

        result_flag = False
        for n in range(len(sqlite_datalist)):
            if (sqlite_datalist[n].id == pg_datalist[n].id and
                    sqlite_datalist[n].name == pg_datalist[n].name and
                    sqlite_datalist[n].description ==
                    pg_datalist[n].description and
                    str(sqlite_datalist[n].created_at)[0:19] ==
                    str(pg_datalist[n].created_at)[0:19] and
                    str(sqlite_datalist[n].updated_at)[0:19] ==
                    str(pg_datalist[n].updated_at)[0:19]):
                result_flag = True
            else:
                result_flag = False

    elif table_name == "person":
        squery = f"SELECT * FROM {table_name} ORDER BY ID LIMIT {row_limit};"
        sqlite_cur.execute(squery)
        sqlite_datalist = [Person(**item) for item in sqlite_cur.fetchall()]

        squery = f"SELECT id, full_name, created as created_at, modified " \
                 f"as updated_at FROM {table_name} " \
                 f"ORDER BY ID LIMIT {row_limit};"
        posg_cur.execute(squery)
        pg_datalist = [Person(**item) for item in posg_cur.fetchall()]

        result_flag = False
        for n in range(len(sqlite_datalist)):
            if (sqlite_datalist[n].id == pg_datalist[n].id and
                    sqlite_datalist[n].full_name ==
                    pg_datalist[n].full_name and
                    str(sqlite_datalist[n].created_at)[0:19] ==
                    str(pg_datalist[n].created_at)[0:19] and
                    str(sqlite_datalist[n].updated_at)[0:19] ==
                    str(pg_datalist[n].updated_at)[0:19]):
                result_flag = True
            else:
                result_flag = False

    elif table_name == "genre_film_work":
        squery = f"SELECT * FROM {table_name} ORDER BY ID LIMIT {row_limit};"
        sqlite_cur.execute(squery)
        sqlite_datalist = [GenreFilmWork(**item) for item in
                           sqlite_cur.fetchall()]

        squery = f"SELECT id, film_work_id, genre_id, " \
                 f"created as created_at FROM {table_name} " \
                 f"ORDER BY ID LIMIT {row_limit};"
        posg_cur.execute(squery)
        pg_datalist = [GenreFilmWork(**item) for item in
                       posg_cur.fetchall()]

        result_flag = False
        for n in range(len(sqlite_datalist)):
            if (sqlite_datalist[n].id == pg_datalist[n].id and
                    sqlite_datalist[n].film_work_id ==
                    pg_datalist[n].film_work_id and
                    sqlite_datalist[n].genre_id ==
                    pg_datalist[n].genre_id and
                    str(sqlite_datalist[n].created_at)[0:19] ==
                    str(pg_datalist[n].created_at)[0:19]):
                result_flag = True
            else:
                result_flag = False

    elif table_name == "person_film_work":
        squery = f"SELECT * FROM {table_name} ORDER BY ID LIMIT {row_limit};"
        sqlite_cur.execute(squery)
        sqlite_datalist = [PersonFilmWork(**item) for item in
                           sqlite_cur.fetchall()]

        squery = f"SELECT id, film_work_id, person_id, role, created as" \
                 f" created_at FROM {table_name} " \
                 f"ORDER BY ID LIMIT {row_limit};"
        posg_cur.execute(squery)
        pg_datalist = [PersonFilmWork(**item) for item in posg_cur.fetchall()]

        result_flag = False
        for n in range(len(sqlite_datalist)):
            if (sqlite_datalist[n].id == pg_datalist[n].id and
                    sqlite_datalist[n].film_work_id ==
                    pg_datalist[n].film_work_id and
                    sqlite_datalist[n].person_id ==
                    pg_datalist[n].person_id and
                    sqlite_datalist[n].role ==
                    pg_datalist[n].role and
                    str(sqlite_datalist[n].created_at)[0:19] ==
                    str(pg_datalist[n].created_at)[0:19]):
                result_flag = True
            else:
                result_flag = False

    return result_flag


def sqlite_db_check():
    sqlitedbfile = '..\\..\\db.sqlite'
    if os.path.exists(sqlitedbfile):
        return True
    return False


def postgres_db_check():
    try:
        cur = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        cur.close()
        return True
    except psycopg2.OperationalError:
        return False


if __name__ == '__main__':
    unittest.main()
