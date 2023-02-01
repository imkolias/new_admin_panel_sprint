import sqlite3
import os
import logging
from dataclasses import astuple, asdict
from contextlib import contextmanager, closing

import psycopg2
from psycopg2.extras import DictCursor, execute_batch

from modules.load_config import dsl, slicesize, datatables_list, \
    colsmatch_list
from modules.dtclass_list import FilmWork, Genre, Person, \
    GenreFilmWork, PersonFilmWork


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
    def __init__(self, sql_cursor, pg_cursor):

        self.pgcurs = pg_cursor
        self.curs = sql_cursor

    def data_insert(self, mdata: list, datatable: str):
        """PUT data in Postgres DB by parts"""
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

        # COMMIT because WITH+CLOSING don't make commit, afrer
        # exit from context manager
        pgc.commit()

    def get_and_push(self):
        """Go through tables list in config DATATABLES_LIST,
        select SLICESIZE from DB, and push DATA_LIST to DATA_INSERT method"""
        logging.info("Start copy process")
        for tablename, configvalue in datatables_list.items():
            row_count = 0
            try:
                sql_query = f"SELECT * FROM {tablename};"
                self.curs.execute(sql_query)
            except:
                logging.error(f'Table "{tablename}" not found, but should be')
            class_name = globals()[configvalue]

            # Process SQL return as BATCH
            while True:
                rowsto_insert = []
                result = self.curs.fetchmany(slicesize)
                if not result:
                    if row_count > 0:
                        logging.info(f'Table: "{tablename}" - '
                                     f'{row_count} row\'s successfully read')
                    else:
                        logging.warning(f'Table: "{tablename}" - '
                                        f'ZERO(0) rows read')
                    break

                # This code get all sql return (SLICESIZE = 500rows),
                # and put it in 'rowsto_insert' list
                for row in result:
                    rowsto_insert += [class_name(**data_reformat(dict(row)))]

                row_count += len(rowsto_insert)

                # Take 'rowsto_insert' (500 rows by default) and send it to
                # POSTGRES DB, using data_insert funct
                # so code use changeable memory count for store sql return
                # and DON'T use memory for all data from table
                self.data_insert(rowsto_insert, tablename)


if __name__ == '__main__':
    sqlitedbfile = 'db.sqlite'

    logging.basicConfig(level=logging.INFO, filename="load_data.log",
                        filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    if os.path.exists(sqlitedbfile):
        logging.info(f'Source SQLite file found: "{sqlitedbfile}"')

        with closing(sqlite3.connect(sqlitedbfile)) as sqlite_conn:
            sqlite_conn.row_factory = sqlite3.Row
            with closing(sqlite_conn.cursor()) as sqlite_cur:

                try:
                    with closing(
                            psycopg2.connect(**dsl,
                                             cursor_factory=DictCursor))\
                            as pgc:
                        with closing(pgc.cursor()) as pgc_cur:

                            logging.info("All connections - OK")
                            sql_to_pg = SQLiteExtractor(sqlite_cur, pgc_cur)
                            sql_to_pg.get_and_push()

                except psycopg2.OperationalError:
                    logging.critical('Can\'t connect to target Postgres DB '
                                     '(OperationalError)')

        logging.info('End of operations')
    else:
        logging.critical(f"Can't find source Sqlite3 DB file: {sqlitedbfile}")
