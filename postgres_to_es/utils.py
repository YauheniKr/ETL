import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

load_dotenv()

DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content'
}

@contextmanager
def conn_context():
    conn = psycopg2.connect(**DSL, cursor_factory=DictCursor)
    yield conn

