import logging
from datetime import datetime
from typing import Generator, List, Tuple

import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import DictCursor

from postgres_to_es.utils import backoff

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PERSON_REQUEST = """
SELECT id, updated_at
FROM content.person
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
LIMIT 100
"""

FILM_REQUEST = """
SELECT fw.id, fw.updated_at
FROM content.film_work fw
LEFT JOIN %(table)s pfw ON pfw.film_work_id = fw.id
WHERE %(field)s IN %(persons)s
ORDER BY fw.updated_at
"""

FILM_REQUEST_PREPARE = """
SELECT
    fw.id as fw_id,
    fw.title,
    fw.description,
    fw.rating,
    pfw.role,
    p.id,
    p.full_name,
    g.name
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN %(films_id)s
"""

GENRE_REQUEST = """
SELECT id, updated_at
FROM content.genre
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
LIMIT 100
"""

FILM_UPDATE_REQUEST = """
SELECT id, updated_at
FROM %(table)s
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
"""
SQL_REQUEST = """
SELECT "content"."film_work"."id","content"."film_work"."title", "content"."film_work"."description",
       "content"."film_work"."rating",
       ARRAY_AGG(DISTINCT "content"."genre"."name" ) AS "genre",
       Json_agg(DISTINCT ("content"."person"."id", "content"."person"."full_name"))
           FILTER (WHERE "content"."person_film_work"."role" = 'actor') AS "actors",
       ARRAY_AGG(DISTINCT "content"."person"."full_name" ) FILTER (WHERE "content"."person_film_work"."role" = 'actor')
           AS "actors_names",
       ARRAY_AGG(DISTINCT ("content"."person"."full_name"))
           FILTER (WHERE "content"."person_film_work"."role" = 'director') AS "director",
       Json_agg(DISTINCT ("content"."person"."id", "content"."person"."full_name") )
           FILTER (WHERE "content"."person_film_work"."role" = 'writer') AS "writers",
       ARRAY_AGG(DISTINCT ("content"."person"."full_name") )
           FILTER (WHERE "content"."person_film_work"."role" = 'writer') AS "writers_names"
FROM "content"."film_work"
    INNER JOIN "content"."genre_film_work"
        ON ("content"."film_work"."id" = "content"."genre_film_work"."film_work_id")
    LEFT OUTER JOIN "content"."genre"
        ON ("content"."genre_film_work"."genre_id" = "content"."genre"."id")
    LEFT OUTER JOIN "content"."person_film_work"
        ON ("content"."film_work"."id" = "content"."person_film_work"."film_work_id")
    LEFT OUTER JOIN "content"."person"
        ON ("content"."person_film_work"."person_id" = "content"."person"."id")
GROUP BY "content"."film_work"."id"
"""


class PostgresExctract:

    def __init__(self, settings):
        self.settings = settings

    def postgres_request(self, sql_request, params=None, n=200) -> Generator:
        """
        Исполняющий метод. По выпонении закрывает соединение
        :param sql_request: запрос для исполнения
        :param params: параметр для подстановки в запрос
        :param n: размер пачки данных запроса возвращаемых через один цикл
        :return:
        """
        self._create_connection()
        self.cursor.execute(sql_request, params)
        while True:
            out = self.cursor.fetchmany(n)
            if not out:
                break
            yield out
        self._close_connection()

    @backoff(logger)
    def _create_connection(self) -> None:
        """
        Создаем соединение
        :return:
        """
        self.connection = psycopg2.connect(**self.settings)
        self.cursor = self.connection.cursor(cursor_factory=DictCursor)

    def _close_connection(self) -> None:
        """
        Проверяем что соединение существует, и курсор не закрыт, то закрываем
        :return:
        """
        if self.connection:
            if not self.cursor.closed:
                self.cursor.close()
            self.connection.close()


def get_film_list_id(postgres: PostgresExctract, data_id: tuple, table: str, field: str) -> Generator:
    """
    :param postgres: экземпляр класса  PostgresExctract для выполнения запроса в БД
    :param data_id: кортеж с id фильмов
    :param table: название тадлицы для линкования(Join)
    :param field: поле по которому буде идти сравнение
    :return:
    """
    film_list = postgres.postgres_request(FILM_REQUEST,
                                          {'persons': data_id, 'table': AsIs(table), 'field': AsIs(field)})
    return film_list


def prepare_filmwork_update(postgres: PostgresExctract, last_request_time: datetime, table: str) -> Generator:
    """
    Функция для отслеживания изменений в таблицах БД
    :param postgres: экземпляр класса  PostgresExctract для выполнения запроса в БД
    :param last_request_time: время последнего запроса
    :param table:название тадлицы для линкования(Join)
    :return:
    """
    params = {'updatedat': last_request_time, 'table': AsIs(table)}
    film_to_update = postgres.postgres_request(FILM_UPDATE_REQUEST, params)
    return film_to_update


def film_get_result_data(postgres: PostgresExctract, films: Tuple[dict]) -> Generator:
    """
    Получаем информацию о фильмах в которых произошли изменения
    :param postgres: экземпляр класса  PostgresExctract для выполнения запроса в БД
    :param films: кортеж словарей с id фиильмов
    :return: Генератор с результатом выполнения запроса
    """
    film_result = postgres.postgres_request(FILM_REQUEST_PREPARE, {'films_id': films})
    return film_result


def get_all_film_to_upload(postgres: PostgresExctract) -> Generator:
    """
    Функция для выполнения запроса получения сведений обо всех фильмах в БД
    :param postgres: экземпляр класса  PostgresExctract для выполнения запроса в БД
    :return: Генератор сс результатом выполнения запроса в БД
    """
    results = postgres.postgres_request(SQL_REQUEST)
    return results


def adopt_request_result(request_result: List[list]) -> List[dict]:
    """
    Функция изменяет ключи в словарях входного списка
    :param request_result: Список словарей для изменения
    :return: Итоговоый список словарей
    """
    out = []
    for result in request_result:
        result = dict(result)
        if result['actors']:
            result['actors'] = [{'id': actor['f1'], 'name': actor['f2']} for actor in result['actors']]
        else:
            result['actors'] = []
        if result['writers']:
            result['writers'] = [{'id': writer['f1'], 'name': writer['f2']} for writer in result['writers']]
        else:
            result['writers'] = []
        out.append(result)
    return out
