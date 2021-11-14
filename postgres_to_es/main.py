import logging
import os
import time
from dataclasses import asdict
from datetime import datetime
from typing import List, Union
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

from postgres_to_es.elastic_loader import ESLoader, load_all_files
from postgres_to_es.extract import (PostgresExctract, film_get_result_data,
                                    get_all_film_to_upload, get_film_list_id,
                                    prepare_filmwork_update)
from postgres_to_es.models import (Filmwork, FilmWorkTables, GenreTables,
                                   Person, PersonTables)
from postgres_to_es.utils import JsonFileStorage, State, backoff

load_dotenv()
URL = os.environ.get('URL')
INDEX = os.environ.get('INDEX')
DSL = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'options': '-c search_path=content'
}
SLEEP_TIME = os.environ.get('SLEEP_TIME')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_films(updated_films: List[dict]) -> List[dict]:
    """
    Траснформируем данные и заполняем аггрегационные поля данными
    :param updated_films: список словарей с данными о фильмах
    :return: список словарей с полями с данными
    """

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
                writers_names=[],
                actors_names=[],
                director=[],
                genre=[])
            out.update({film['fw_id']: asdict(film_class)})
        film_class = out.get(film['fw_id'])
        person = Person(id=film['id'], name=film['full_name'])
        if film['role'] == 'actor' and film['full_name'] not in film_class['actors_names']:
            film_class['actors_names'].append(film['full_name'])
            film_class['actors'].append(asdict(person))
        elif film['role'] == 'writer' and film['full_name'] not in film_class['writers_names']:
            film_class['writers_names'].append(film['full_name'])
            film_class['writers'].append(asdict(person))
        elif film['role'] == 'director' and film['full_name'] not in film_class['director']:
            film_class['director'].append(film['full_name'])
        if film['name'] not in film_class['genre']:
            film_class['genre'].append(film['name'])
    return list(out.values())


def exchange_app(postgres_request: PostgresExctract, request_time: datetime,
                 table: Union[PersonTables, GenreTables, FilmWorkTables]) -> List[dict]:
    """
    Мониторинг обновлений в БД
    :param postgres_request: экземпляр класса  PostgresExctract для выполнения запроса в БД
    :param request_time: время последнего запроса
    :param table: список таблиц для передачи в запрос
    :return: данные об обновлениях
    """
    get_updated_data = prepare_filmwork_update(postgres_request, request_time, table.table_to_check_update)
    get_updated_data = [dict(d) for data in get_updated_data for d in data]
    id_data = tuple([updated_data['id'] for updated_data in get_updated_data])
    ready_update = {}
    if get_updated_data and table.table_list[1]:
        film_list_id = get_film_list_id(postgres_request, id_data, *table.table_list[1:])
        id_data = tuple(dict(f) for films in film_list_id for f in films)
    if id_data:
        films_result = film_get_result_data(postgres_request, id_data)
        films_result = [dict(f) for films in films_result for f in films]
        ready_update = transform_films(films_result)
    return ready_update


@backoff(logger)
def check_index(index_name: str) -> bool:
    """
    Проверяем наличие индекса в эластик
    :param index_name: Имян индекса
    :return: True/False
    """
    url = urljoin(URL, index_name),
    response = requests.get(*url, headers={'Content-Type': 'application/json'})
    if response.status_code == 200:
        logger.info(f'Схема индекса {index_name} уже существует')
        return True
    return False


def main():
    # Инициализируем экземплары классов
    esl = ESLoader(URL)
    index_status = check_index(INDEX)
    postgres_request = PostgresExctract(DSL)
    person_tables = PersonTables(['content.person', 'content.person_film_work', 'pfw.person_id'])
    genre_tables = GenreTables(['content.genre', 'content.person_film_work', 'pfw.genre_id'])
    firlmwork_tables = FilmWorkTables(['content.film_work', ''])

    # проверяем наличие схемы и если еет создаем и переливаем все файлы из БД
    if not index_status:
        logger.info(f'Схемы {INDEX} не существует. Создаем схему')
        esl.create_index(INDEX)
        logger.info('Получаем данные о фильмах.')
        film_data = get_all_film_to_upload(postgres_request)
        logger.info(f'Загружаем данные о фильмах в Схему {INDEX}.')
        load_all_files(film_data, esl, INDEX)
    # Инициализируем хранилище и хранение состояния
    json_storage = JsonFileStorage('sw_templates.json')
    state = State(json_storage)

    # Список классов определяющих таблицы для проверки
    table_list_to_check = [person_tables, genre_tables, firlmwork_tables]

    # цикл для сканирования обновлений
    while True:
        for table in table_list_to_check:
            if not state.get_state(table.table_to_check_update):
                run_time = datetime.now()
            else:
                run_time = state.get_state(table.table_to_check_update)
            prepared_data = exchange_app(postgres_request, run_time, table)
            if prepared_data:
                logger.info('Заливаем изменения в Elastic')
                esl.load_to_es(prepared_data, INDEX)
            check_time = datetime.now().isoformat()
            state.set_state(table.table_to_check_update, check_time)
        logger.info(f'Засыпаем на {SLEEP_TIME} сек.')
        time.sleep(SLEEP_TIME)


if __name__ == '__main__':
    main()
