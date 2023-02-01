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

# Config dict for table-dataclass matching
# table_name: DataClass
datatables_list = {
    "film_work": "FilmWork",
    "genre": "Genre",
    "person": "Person",
    "genre_film_work": "GenreFilmWork",
    "person_film_work": "PersonFilmWork"
}

# table names matching between SOURCE and TARGET col names
# source_table_col_name:target_table_col_name
colsmatch_list = {
    "created_at": "created",
    "updated_at": "modified",
    "file_path": ""
}


if __name__ == '__main__':
    pass
