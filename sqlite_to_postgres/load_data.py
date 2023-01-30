import sqlite3
import os
import psycopg2

from dataclasses import astuple, fields
from modules.load_config import dsl
from modules.dtclass_list import DataContainer, Movie, Genre, Person, PersonFilmWork, GenreFilmWork
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from itertools import islice


pgconfig_dict = {"film_work": ["type, title, description, creation_date, created, modified, rating, id", 8],
               "genre": ["name, description, created, modified, id", 5],
               "person": ["full_name, created, modified, id", 4],
               "genre_film_work": ["film_work_id, genre_id, created, id", 4],
               "person_film_work": ["film_work_id, person_id, role, created, id", 5]
               }

sqltconfig_dict = {"film_work": ["type, title, description, creation_date, created_at, updated_at, rating, id", 'Movie'],
               "genre": ["name, description, created_at, updated_at, id", 'Genre'],
               "person": ["full_name, created_at, updated_at, id", 'Person'],
               "genre_film_work": ["film_work_id, genre_id, created_at, id", 'GenreFilmWork'],
               "person_film_work": ["film_work_id, person_id, role, created_at, id", 'PersonFilmWork']
               }

slicesize = 200

@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()

def sqliterator(cursor, arraysize=200):
        results = cursor.fetchmany(arraysize)
        yield results


class PostgresSaver:
    """Save to Postgres DB class

        save_all_data() - main class to proccess all tables inside DB
                          run all workers

    """

    def __init__(self, pgcon: _connection):
        self.curs = pgcon.cursor()


    def equality_check(self, count: int, table_name: str):
        """ Check equality between COUNT var and real rows count in TABLE_NAME
            return OK or ERROR message"""
        self.curs.execute(f"SELECT COUNT(id) FROM content.{table_name};")
        saved_count = (self.curs.fetchone())
        if count == int(saved_count[0]):
            print('[OK] saved:', saved_count[0])
        else:
            print('[ERROR] wrong save count, should be:', count,
                  'saved:', saved_count[0])
        print()

    def save_all_data(self, data: DataContainer):
        """ Process data, GET from DATA container and then PUT in Postgres"""
        for num, field in enumerate(fields(data)):

            key = list(pgconfig_dict.keys())[num]
            value = list(pgconfig_dict.values())[num]

            self.data_insert(getattr(data, field.name), key, value)
        pass

    def data_insert(self, mdata: list, datatable:str, config: list):

        """PUT data in Postgres DB by parts, using PGCONFIG_DICT"""
        print(f'    [Save {datatable} table in db]')
        elems_count = len(mdata)
        times = elems_count // slicesize
        print('rows to insert:', elems_count)
        # for time in range(times + 1):
        #     print('.', end='')

        sql_params = []
            # for elem in islice(mdata, 0 + (time * slicesize),
            #                    slicesize + (time * slicesize)):
            #     sql_params += ((astuple(elem)),)
        for elem in mdata:
            sql_params += ((astuple(elem)),)
            break
        print(sql_params)

        vars_count = ", ".join("%s" for _ in range(config[1]))
        sql_query = f"INSERT INTO content.{datatable} ({config[0]}) VALUES ({vars_count}) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id;"
        print(sql_query)
        self.curs.executemany(sql_query, sql_params)
        self.equality_check(elems_count, datatable)


class SQLiteExtractor:
    """
        SQLiteExtractor - class to read selected .sqlite file
                          find predefined Tables.

                          There are 5 workers, one for
                          each table, this methods return
                          containers
    """
    curs = 0

    def __init__(self, connection: sqlite3.Connection, pgcon: _connection):
        self.pgcurs = pgcon.cursor()
        self.curs = connection.cursor()
        print('[Start read data from SQLite db]')


    def data_insert(self, mdata: list, datatable:str, config: list):

        """PUT data in Postgres DB by parts, using PGCONFIG_DICT"""
        print(f'    [Save {datatable} table in db]')
        elems_count = len(mdata)
        times = elems_count // slicesize
        print('rows to insert:', elems_count)


        sql_params = []
        for elem in mdata:
            sql_params += ((astuple(elem)),)

        vars_count = ", ".join("%s" for _ in range(config[1]))
        sql_query = f"INSERT INTO content.{datatable} ({config[0]}) VALUES ({vars_count}) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id;"
        self.pgcurs.executemany(sql_query, sql_params)



    def extract_all_data(self):

        # Тут я должен сделать цикл основной который будет идти по таблица
        for tablename, configvalue in sqltconfig_dict.items():

            print(f'    [Get data] from {tablename}')
            sql_query = f"SELECT {configvalue[0]} FROM {tablename};"
            self.curs.execute(sql_query)

            iter_n = 0
            class_name=globals()[configvalue[1]]
            # print(class_name)
            while True:
                data_list = []
                result = self.curs.fetchmany(100)
                if not result:
                    break
                for row in result:
                    data_list += [class_name(*row)]

                # print("ITER", tablename)
            # here i should push this
            # print(data_list)
            # exit()
            # print(pgconfig_dict[tablename])
            # print("SEL:",type(data_list), data_list)
                self.data_insert(data_list, tablename, pgconfig_dict[tablename])

            # exit()




def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """
        load_from_sqlite : Main class, start all work.
                           First: open connections to DB's
                           Second: run Extract data from sqlite,
                           put them on special datacontainer
                           Third and final: run Data saver,
                           insert all extracted data to
                           Postrges DB
    """
    print("[All connections - ok]")
    # postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection, pg_conn)


    sqlite_extractor.extract_all_data()

    print()
    # postgres_saver.save_all_data(data)


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
