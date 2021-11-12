import json
import logging
from typing import Generator, List
from urllib.parse import urljoin

import requests

from .extract import adopt_request_result
from .utils import backoff

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        Подготавливает bulk-запрос в Elasticsearch
        :param rows: Список словарей с данными
        :param index_name: название индекса
        :return: Список строк подготовленный для загрузки
        """
        prepared_query = []

        for row in rows:
            prepared_query.extend([json.dumps({'index': {'_index': index_name, '_id': row['id']}}), json.dumps(row)])
        return prepared_query

    @backoff(logger)
    def load_to_es(self, records: List[dict], index_name: str) -> None:
        """
        Отправка запроса в ES и разбор ошибок сохранения данных
        :param records: Список словарей с данными
        :param index_name: название индекса
        :return: None
        """
        prepared_query = self._get_es_bulk_query(records, index_name)
        adopted_query = '\n'.join(prepared_query) + '\n'
        response = requests.post(urljoin(self.url, '_bulk'), data=adopted_query,
                                 headers={'Content-Type': 'application/json'}
                                 )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)

    def __get_index_data(self, filename='schema.json'):
        """
        Получение схемы данных из json файла
        """
        with open(filename) as file:
            return json.load(file)

    def create_index(self, index: str):
        """
        Создаем индекс в Elastic
        :param index: схема данных в формате json
        :return: результат выполнения request
        """
        index_schema = json.dumps(self.__get_index_data())
        url = urljoin(self.url, index)
        create_response = requests.request('PUT', url, headers={'Content-Type': 'application/json'}, data=index_schema)
        logger.info(f'Схема {index} успешно создана')
        return create_response


def load_all_files(film_data: Generator, esl: ESLoader, index: str) -> None:
    """
    :param film_data: данные для загрузки в виде списка словарей
    :param esl: экземпляр класса ESLoader
    :param index:название схемы данных для загрузки в elastic
    :return:
    """
    for film_pack in film_data:
        bulk_film = adopt_request_result(film_pack)
        esl.load_to_es(bulk_film, index)
