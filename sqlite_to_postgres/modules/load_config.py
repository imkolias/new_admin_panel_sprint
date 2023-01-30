from dotenv import load_dotenv
import os

load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content',
}

# table_list = ['film_work', 'genre', 'persons', 'genre_film_work', 'person_film_work']


if __name__ == 'main':
    print("[Warning] It's module file, not for run!")