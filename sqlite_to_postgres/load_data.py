import sqlite3
import os
import psycopg2

from dataclasses import astuple, asdict
from modules.load_config import dsl, slicesize, datatables_list, \
    colsmatch_list
from modules.dtclass_list import *
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch
from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def data_reformat(datadict: dict) -> dict:
    """ Get DATADICT data (one row from sql select) and proccess it
    replace col names as in config COLSMATCH_LIST, and delete
    col if no new name for it"""
    for oldcolname, newcolname in colsmatch_list.items():
        if newcolname != "":
            if oldcolname in datadict.keys():
                datadict[newcolname] = datadict[oldcolname]
                del (datadict[oldcolname])
        else:
            if oldcolname in datadict.keys():
                del (datadict[oldcolname])

    return datadict


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

    def data_insert(self, mdata: list, datatable: str):
        """PUT data in Postgres DB by parts"""
        print('[W]', end="")
        sql_params = []
        for elem in mdata:

            sql_params += (astuple(elem),)

        cols_names = ", ".join(col for col in asdict(mdata[0]).keys())
        cols_count = len(asdict(mdata[0]).keys())

        vars_count = ', '.join('%s' for _ in range(cols_count))
        sql_query = f"INSERT INTO content.{datatable} ({cols_names}) VALUES" \
                    f" ({vars_count}) ON CONFLICT (id) " \
                    f"DO UPDATE SET id=EXCLUDED.id;"

        execute_batch(self.pgcurs, sql_query, sql_params)

    def extract_all_and_push(self):
        """Go through tables list in config DATATABLES_LIST,
        select SLICESIZE from DB, and push DATA_LIST to DATA_INSERT method"""
        print("[Start copy process]")

        for tablename, configvalue in datatables_list.items():
            row_count = 0
            print(f'[SELECT TABLE] "{tablename}"')
            sql_query = f"SELECT * FROM {tablename};"
            self.curs.execute(sql_query)
            print("    ", end="")
            class_name = globals()[configvalue]

            while True:
                print('[R]', end="")
                data_list = []
                result = self.curs.fetchmany(slicesize)
                if not result:
                    print(f' ({row_count} rows writed)')
                    break
                for row in result:
                    data_list += [class_name(**data_reformat(dict(row)))]

                row_count += len(data_list)

                self.data_insert(data_list, tablename)


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
