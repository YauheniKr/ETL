import os
from dataclasses import asdict
from datetime import datetime, timedelta

import psycopg2 as psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from postgres_to_es.elastic_loader import ESLoader
from postgres_to_es.extract import Postgres_Exctract, \
    get_film_list_id, prepare_filmwork_update, film_get_result_data  # , prepare_person_update, prepare_genre_update, \

from postgres_to_es.models import Filmwork, Person

load_dotenv()
DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content'
}
last_request_time = (datetime.now() - timedelta(days=1))


def transform_films(updated_films):
    out = {}
    for film in updated_films:
        if not out.get(film['fw_id']):
            film_class = Filmwork(
                id=film['fw_id'],
                title=film['title'],
                rating=film['rating'],
                description=film['description'],
                writers=[],
                actors=[],
                writers_name=[],
                actors_name=[],
                director=[],
                genre=[])
            out.update({film['fw_id']: asdict(film_class)})
        film_class = out.get(film['fw_id'])
        person = Person(id=film['id'], name=film['full_name'])
        if film['role'] == 'actor' and film['full_name'] not in film_class['actors_name']:
            film_class['actors_name'].append(film['full_name'])
            film_class['actors'].append(asdict(person))
        elif film['role'] == 'writer' and film['full_name'] not in film_class['writers_name']:
            film_class['writers_name'].append(film['full_name'])
            film_class['writers'].append(asdict(person))
        elif film['role'] == 'director' and film['full_name'] not in film_class['director']:
            film_class['director'].append(film['full_name'])
        if film['name'] not in film_class['genre']:
            film_class['genre'].append(film['name'])
    return out


def exchange_app(pg_conn: _connection):
    postgres_request = Postgres_Exctract(pg_conn)
    table_list_to_check = [['content.person', 'content.person_film_work', 'pfw.person_id'],
                           ['content.genre', 'content.person_film_work', 'pfw.genre_id'], ['content.film_work', '']
                           ]
    for table in table_list_to_check:
        get_updated_data = prepare_filmwork_update(postgres_request, last_request_time, table[0])
        id_data = tuple([updated_data['id'] for updated_data in get_updated_data])
        ready_update = {}
        if get_updated_data and table[1]:
            film_list_id = get_film_list_id(postgres_request, id_data, *table[1:])
            id_data = tuple([films['id'] for films in film_list_id])
        if id_data:
            films_result = film_get_result_data(postgres_request, id_data)
            ready_update = transform_films(films_result)
    print(ready_update)




if __name__ == '__main__':
    es = ESLoader('http://192.168.50.17:9200/')
    print(es.check_index('movies'))
    #with psycopg2.connect(**DSL, cursor_factory=DictCursor) as pg_conn:
    #    exchange_app(pg_conn)
        # pg_conn.close()
