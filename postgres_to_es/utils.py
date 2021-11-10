import abc
import json
import logging
import os
import time
from typing import Any, Optional

import psycopg2
from urllib3.exceptions import NewConnectionError, MaxRetryError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict):
        with open(self.file_path, 'w') as file:
            json.dump(state, file, indent=4)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        if not os.path.isfile(self.file_path):
            return {}
        with open(self.file_path, 'r') as file:
            try:
                templates = json.load(file)
            except json.decoder.JSONDecodeError:
                templates = {}
            return templates


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        data = self.storage.retrieve_state()
        data[key] = value
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        data = self.storage.retrieve_state()
        return data.get(key)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def real_decorator(func):
        def inner(*args, **kwargs):
            i = 0
            t = start_sleep_time * factor ** i
            while True:
                try:
                    logger.info('Пытаемся подключится')
                    out = func(*args, **kwargs)
                    logger.info('Подключение успешно выполнено')
                    return out
                except(psycopg2.Error, TimeoutError, NewConnectionError, ConnectionError, MaxRetryError,
                        ConnectionRefusedError) as e:
                    logger.error(f'Произошла ошибка {e} при подключении')
                    time.sleep(t)
                    i += 1
                    if t < border_sleep_time:
                        t = start_sleep_time * 2 ** i
                    else:
                        t = border_sleep_time

        return inner

    return real_decorator
