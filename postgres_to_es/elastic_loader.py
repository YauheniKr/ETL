import json
import logging
from typing import List
from urllib.parse import urljoin

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        '''
        Подготавливает bulk-запрос в Elasticsearch
        '''
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        '''
        Отправка запроса в ES и разбор ошибок сохранения данных
        '''
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


def __get_index_data():
    with open('schema.txt') as file:
        return json.load(file)


def create_index(url, index):
    index_schema = json.dumps(__get_index_data())
    url = urljoin(url, index)
    create_response = requests.request('PUT', url, headers={'Content-Type': 'application/json'}, data=index_schema)
    logger.info(f'Схема {index} успешно создана')
    return create_response
