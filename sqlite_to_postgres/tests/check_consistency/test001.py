import sqlite3
import psycopg2
import os
import pytest
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection as _connection
from dotenv import load_dotenv
from contextlib import contextmanager



load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content',
}

tables_list = ['film_work', 'person', 'genre', 'genre_film_work', 'person_film_work']

@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()

def runtests(sqlite_con: sqlite3.Connection, pgs_con: _connection):
    sqlite_cur = sqlite_con.cursor()
    posg_cur = pgs_con.cursor()

    # Row count equality check
    print("[Start row count equality between DB's]")
    for table_name in tables_list:
        squery = f"SELECT count(id) FROM {table_name};"
        sqlite_cur.execute(squery)
        posg_cur.execute(squery)
        sqlite_cnt = sqlite_cur.fetchone()[0]
        pg_cnt = posg_cur.fetchone()[0]


        print("  Table -", table_name, ":", sqlite_cnt, "/", pg_cnt, resultstr)



if __name__ == '__main__':
    sqlitedbfile = '..\..\db.sqlite'

    if os.path.exists(sqlitedbfile):

        try:
            with conn_context(sqlitedbfile) as sqlite_conn, \
                    psycopg2.connect(**dsl, cursor_factory=DictCursor) as pgc:

                runtests(sqlite_conn, pgc)

        except psycopg2.OperationalError:
            print('[Error] - Can\'t connect to Postgres DB')

    else:
        print('[Error] - Can\'t find source Sqlite3 DB file:', sqlitedbfile)
