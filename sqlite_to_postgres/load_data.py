import sqlite3
import os
import psycopg2

from dataclasses import astuple
from modules.load_config import dsl, slicesize, pgconfig_dict, sqltconfig_dict
from modules.dtclass_list import *
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def data_reformat(datadict: dict) -> dict:





class SQLiteExtractor:
    """
        SQLiteExtractor - class to read selected .sqlite file
                          and insert in target Postgres.

                          extract_all_and_push - go through all tables,
                          and push data to data_insert

                          data_insert - insert data in target table
    """
    def __init__(self, connection: sqlite3.Connection, pgcon: _connection):
        self.pgcurs = pgcon.cursor()
        self.curs = connection.cursor()
        print('[Class init complete]')

    def data_insert(self, mdata: list, datatable: str, config: list):
        """PUT data in Postgres DB by parts, using PGCONFIG_DICT"""
        print('[W]', end="")

        sql_params = []
        for elem in mdata:
            sql_params += ((astuple(elem)),)

        vars_count = ', '.join('%s' for _ in range(config[1]))
        sql_query = f"INSERT INTO content.{datatable} ({config[0]}) VALUES" \
                    f" ({vars_count}) ON CONFLICT (id) " \
                    f"DO UPDATE SET id=EXCLUDED.id;"

        self.pgcurs.executemany(sql_query, sql_params)

    def extract_all_and_push(self):
        """Go through tables list in config SQLTCONFIG_DICT,
        select SLICESIZE from it, and push DATA_LIST to DATA_INSERT method"""
        print("[Start copy process]")

        for tablename, configvalue in sqltconfig_dict.items():
            row_count = 0
            print(f'[SELECT TABLE] "{tablename}"')
            sql_query = f"SELECT {configvalue[0]} FROM {tablename};"
            self.curs.execute(sql_query)
            print("    ", end="")
            class_name = globals()[configvalue[1]]
            while True:
                print('[R]', end="")
                data_list = []
                result = self.curs.fetchmany(slicesize)
                if not result:
                    print(f' ({row_count} rows writed)')
                    break
                for row in result:
                    data_list += [class_name(*row)]
                row_count += len(data_list)

                self.data_insert(data_list, tablename,
                                 pgconfig_dict[tablename])


def movedata(connection: sqlite3.Connection, pg_conn: _connection):
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
    sqlite_extractor = SQLiteExtractor(connection, pg_conn)

    sqlite_extractor.extract_all_and_push()


if __name__ == '__main__':
    sqlitedbfile = 'db.sqlite'

    if os.path.exists(sqlitedbfile):

        try:
            with conn_context(sqlitedbfile) as sqlite_conn, \
                    psycopg2.connect(**dsl, cursor_factory=DictCursor) as pgc:
                movedata(sqlite_conn, pgc)
        except psycopg2.OperationalError:
            print('[Error] - Can\'t connect to Postgres DB')

    else:
        print('[Error] - Can\'t find source Sqlite3 DB file:', sqlitedbfile)
