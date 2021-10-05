import json
import logging
from collections import namedtuple
from time import sleep
from typing import Any
from urllib.parse import urljoin

import backoff
import psycopg2
import redis
import requests
from decouple import config

from state_storage import RedisStorage
from utils import coroutine
from filmwork_adapter import FilmWorkAdapter

logging.basicConfig(level=logging.INFO)


class ETL:
    UpdatedFilmworks = namedtuple('UpdatedFilmworksData', 'data state_name updated_state_value')

    def __init__(self,
                 redis_storage: Any,
                 filmwork_adapter: Any,
                 index_name: str,
                 elastic_url: str):

        self.filmwork_adapter = filmwork_adapter
        self.redis_storage = redis_storage
        self.index_name = index_name
        self.elastic_url = elastic_url

    def __get_es_bulk_query(self, rows: list[dict]) -> list[str]:
        """
        Создаем список для записи в ES.
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': self.index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout))
    def __load_to_es(self, records: list[dict]) -> None:
        prepared_query = self.__get_es_bulk_query(records)
        str_query = '\n'.join(prepared_query) + '\n'
        response = requests.post(
            urljoin(self.elastic_url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logging.error(error_message)

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def get_updated_table_data(self, state_name: str) -> UpdatedFilmworks:
        """
        Получаем обновленные фильмы по названию таблицы. Если state_name = filmwork, то просто делаем запрос на
        получение всех фильмов по последнему состоянию updated_at этой таблицы. Если state_name in [preson, genre],
        то сначала получим все обновленные объкты этих таблицы по состоянию updated_at, а  потом возьмем все FilmWork,
        в которых встречаются эти обновленные объекты.
        """
        pg_conn = self.filmwork_adapter.get_db_connection()
        try:
            postgres_cur = pg_conn.cursor()
            state = self.redis_storage.retrieve_state(state_name)
            state = state if state else config('DEFAULT_UPDATED_AT_STATE')
            if state_name != self.redis_storage.states[0]:
                postgres_cur.execute(self.filmwork_adapter.get_updated_table_ids_sql(state, state_name))
            else:
                postgres_cur.execute(self.filmwork_adapter.get_updated_filmworks_sql(state))
            data = postgres_cur.fetchall()
            updated_state_value = None
            if data:
                updated_state_value = data[-1]['updated_at']
                if state_name != self.redis_storage.states[0]:
                    ids = ','.join([f"'{row['id']}'" for row in data])
                    postgres_cur.execute(self.filmwork_adapter.get_updated_filmworks_ids_by_state_name(ids, state_name))
                    updated_filmworks_ids_data = postgres_cur.fetchall()
                    updated_filmworkds_ids = ','.join([f"'{filmwork['id']}'" for filmwork in updated_filmworks_ids_data])
                    postgres_cur.execute(self.filmwork_adapter.get_updated_filmworks_by_ids_sql(updated_filmworkds_ids))
                    data = postgres_cur.fetchall()
        finally:
            pg_conn.close()
        updated_filmworks = self.UpdatedFilmworks(data=data,
                                                  state_name=state_name,
                                                  updated_state_value=updated_state_value)
        return updated_filmworks

    @coroutine
    def extract(self, target):
        while True:
            sleep(5)
            updated_filmworks = [self.get_updated_table_data(self.redis_storage.states[2]),
                                 self.get_updated_table_data(self.redis_storage.states[1]),
                                 self.get_updated_table_data(self.redis_storage.states[0])]
            for updated_table_filmworks in updated_filmworks:
                if updated_table_filmworks.data:
                    target.send(updated_table_filmworks)
                    logging.info(f'Found {len(updated_table_filmworks.data)} updated filmworks of table {updated_table_filmworks.state_name}')

    @coroutine
    def transform(self, target):
        while True:
            updated_table_filmwork = (yield)
            index_film_data = []
            for film in updated_table_filmwork.data:
                genres = film['genres_titles'].split(',')
                actors = [
                    {'id': _id, 'name': name}
                    for _id, name in zip(film['actors_ids'].split(','), film['actors_names'].split(','))
                ]
                writers = [
                    {'id': _id, 'name': name}
                    for _id, name in zip(film['writers_ids'].split(','), film['writers_names'].split(','))
                ]
                index_film = {
                    'id': film['id'],
                    'genres': genres,
                    'imdb_rating': float(film['rating']),
                    'title': film['title'],
                    'description': film['description'],
                    'directors': film['directors_names'].split(','),
                    'actors_names': film['actors_names'].split(','),
                    'writers_names': film['writers_names'].split(','),
                    'actors': actors,
                    'writers': writers
                }
                index_film_data.append(index_film)
            transformed_filmworks = self.UpdatedFilmworks(data=index_film_data,
                                                          state_name=updated_table_filmwork.state_name,
                                                          updated_state_value=updated_table_filmwork.updated_state_value)
            target.send(transformed_filmworks)

    @coroutine
    def load(self):
        while True:
            transformed_filmworks = (yield)
            self.__load_to_es(transformed_filmworks.data)
            if transformed_filmworks.data:
                self.redis_storage.save_state(transformed_filmworks.state_name,
                                              str(transformed_filmworks.updated_state_value))
            logging.info(f"{len(transformed_filmworks.data)} filmworks loaded to Elastic index")


if __name__ == '__main__':
    input_dsn = {'dbname': config('DB_NAME'),
                 'user': config('DB_USER'),
                 'password': config('DB_PASSWORD'),
                 'host': config('DB_HOST') or 'localhost',
                 'port': config('DB_PORT') or '5432'
                 }

    r = redis.Redis(host=config('REDIS_HOST'), port=config('REDIS_PORT'))
    input_redis_storage = RedisStorage(redis_adapter=r)
    input_filmwork_adapter = FilmWorkAdapter(dsn=input_dsn,
                                             chunk_size=config('CHUNK_SIZE'))
    etl = ETL(filmwork_adapter=input_filmwork_adapter,
              redis_storage=input_redis_storage,
              elastic_url=config('ELASTIC_URL'),
              index_name=config('ELASTIC_INDEX_NAME')
              )
    logging.info("Let's go")
    load_process = etl.load()
    transform_process = etl.transform(load_process)
    extract_process = etl.extract(target=transform_process)
