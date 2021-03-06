# ETL: Сервис переноса данных из БД Postgresql в Elasticsearch

Сервис создает соответствующий индекс в Elasticsearch и переносит данные из  Postgresql в Elasticsearch в 
созданный индекс. После переноса сервис переходит в режим сканирования и при наличии обновлений в БД Postgresql 
автоматически обновляет данные в Elasticsearch.

Для запуска проекта необходимо склонировать репозиторий на локальную машину и установить необходимые модули указанные в 
файле requirements.txt

Так же в каталоге postgres_to_es необходимо создать .env файл со следующими переменными окружения:

DB_NAME - имя базы данных

POSTGRES_USER - пользователь для доступа к БД

POSTGRES_PASSWORD - доступа к БД

DB_HOST - адрес на котором развернута БД postgresql

DB_PORT - номер порта на котором работает БД postgresql

URL - адрес и порт на котором работает elasticsearch

INDEX - название индекса для загрузки данных в Elasticsearch

SLEEP_TIME - время сна между сканированиями базы данных

Схема индекса по умолчанию считывается из файла schema.json

