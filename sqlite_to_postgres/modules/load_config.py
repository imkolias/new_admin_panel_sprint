from dotenv import load_dotenv
import os

load_dotenv()

# Load ENV vars and use it as params for DB
dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content',
}

# Size of each slize that will be SELECTED and then INSERTED in to target DB
slicesize = 500

# Config dicts for table matching
# config for POSTGRES table, format:
# table_name: [table cols names in table ordered like in right dataclass,
# table cols number]
# pgconfig_dict = {
#     "film_work": ["type, title, description, creation_date, created, modified,"
#                   " rating, id", 8],
#     "genre": ["name, description, created, modified, id", 5],
#     "person": ["full_name, created, modified, id", 4],
#     "genre_film_work": ["film_work_id, genre_id, created, id", 4],
#     "person_film_work": ["film_work_id, person_id, role, created, id", 5]
#                }
#
# # config for SQLite DB table
# # table_name: [table cols names in table ordered like in right dataclass,
# # DATACLASS name]
# # sqltconfig_dict = {
# #     "film_work": ["type, title, description, creation_date, created_at, "
# #                   "updated_at, rating, id", 'Movie'],
# #     "genre": ["name, description, created_at, updated_at, id", 'Genre'],
# #     "person": ["full_name, created_at, updated_at, id", 'Person'],
# #     "genre_film_work": ["film_work_id, genre_id, created_at, id",
# #                         'GenreFilmWork'],
# #     "person_film_work": ["film_work_id, person_id, role, created_at,"
# #                          " id", 'PersonFilmWork']
# #                }


datatables_list = {
    "film_work": "FilmWork",
    "genre": "Genre",
    "person": "Person",
    "genre_film_work": "GenreFilmWork",
    "person_film_work": "PersonFilmWork"
}


if __name__ == '__main__':
    print("[Warning] It's module file, not for run!")
