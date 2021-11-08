import json
from datetime import datetime, timedelta
from typing import List

from psycopg2.extensions import AsIs

last_request_time = str(datetime.now() - timedelta(days=1))

PERSON_REQUEST = f"""
SELECT id, updated_at
FROM content.person
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
LIMIT 100
"""

FILM_REQUEST = f"""
SELECT fw.id, fw.updated_at
FROM content.film_work fw
LEFT JOIN %(table)s pfw ON pfw.film_work_id = fw.id
WHERE %(field)s IN %(persons)s
ORDER BY fw.updated_at
"""

FILM_REQUEST_PREPARE = f"""
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

GENRE_REQUEST = f"""
SELECT id, updated_at
FROM content.genre
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
LIMIT 100
"""

FILM_UPDATE_REQUEST = f"""
SELECT id, updated_at
FROM %(table)s
WHERE updated_at > %(updatedat)s
ORDER BY updated_at
LIMIT 100
"""
SQL_REQUEST = """
SELECT "content"."film_work"."id","content"."film_work"."title", "content"."film_work"."description",
       "content"."film_work"."rating", "content"."film_work"."type",
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


class Postgres_Exctract:

    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def dictfetchall(self):
        columns = [col[0] for col in self.cursor.description]
        data = self.cursor.fetchall()
        return [dict(zip(columns, row)) for row in data]

    def postgres_request(self, sql_request, params=None):
        self.cursor.execute(sql_request, params)
        out = self.dictfetchall()
        return out


def get_film_list_id(postgres: Postgres_Exctract, data_id, table, field):
    film_list = postgres.postgres_request(FILM_REQUEST,
                                          {'persons': data_id, 'table': AsIs(table), 'field': AsIs(field)})
    return film_list


def prepare_filmwork_update(postgres: Postgres_Exctract, last_request_time: datetime, table) -> List[dict]:
    params = {'updatedat': last_request_time, 'table': AsIs(table)}
    film_to_update = postgres.postgres_request(FILM_UPDATE_REQUEST, params)
    return film_to_update


def film_get_result_data(postgres: Postgres_Exctract, films):
    film_result = postgres.postgres_request(FILM_REQUEST_PREPARE, {'films_id': films})
    return film_result


def get_all_film_to_upload(postgres: Postgres_Exctract):
    results = postgres.postgres_request(SQL_REQUEST)
    for result in results:
        if result['actors']:
            result['actors'] = [{'id':actor['f1'], 'fullname':actor['f2']} for actor in result['actors']]
        else:
            result['actors'] = []
        if result['writers']:
            result['writers'] = [{'id': writer['f1'], 'fullname': writer['f2']} for writer in result['writers']]
        else:
            result['writers'] = []
    return results
