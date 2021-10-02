import abc
import json
import logging
from functools import wraps
from time import sleep
from typing import Any
from urllib.parse import urljoin

import psycopg2
import redis
import requests
from decouple import config
from psycopg2.extras import RealDictCursor
from redis import Redis

logging.basicConfig(level=logging.INFO)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    print(ex)
                    t += start_sleep_time * 2**(factor) if t < border_sleep_time else border_sleep_time
                    sleep(t)
        return inner
    return func_wrapper


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn
    return inner


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, key: str, value: str) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self, key: str) -> dict:
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        self.states = ['filmwork', 'genre', 'person']

    def save_state(self, key: str, value: str) -> None:
        self.redis_adapter.set(key, value)

    def retrieve_state(self, key) -> dict:
        value = self.redis_adapter.get(key)
        return value if not value else value.decode("UTF-8")


class ETL:
    def __init__(self, dsn: dict,
                 redis_storage: Any,
                 chunk_size: int,
                 index_name: str,
                 elastic_url: str):

        self.dsn = dsn
        self.chunk_size = chunk_size
        self.redis_storage = redis_storage
        self.index_name = index_name
        self.elastic_url = elastic_url

    def __get_sql_for_m2m_person(self, table_name: str) -> str:
        with_name = table_name.split('_')[2]
        as_name = ''.join([word[0] for word in table_name.split('_')])
        SQL = f'''
        {with_name} as (
        SELECT m.id, 
               string_agg(CAST(a.id AS TEXT), ',') ids,
               string_agg(a.first_name || ' ' || a.last_name, ',') persons_names
        FROM movies_filmwork m
            LEFT JOIN {table_name} {as_name} on m.id = {as_name}.filmwork_id 
            LEFT JOIN movies_person a on {as_name}.person_id = a.id
        GROUP BY m.id
        )
        '''
        return SQL

    def __get_es_bulk_query(self, rows: list[dict]) -> list[str]:
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': self.index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def __get_updated_table_ids_sql(self, updated_at: str, state_name: str) -> str:
        SQL = f'''
        SELECT id,
               updated_at 
        FROM movies_{state_name} 
        WHERE updated_at > '{updated_at}'
        ORDER BY updated_at
        LIMIT {self.chunk_size}
        '''
        return SQL

    def __get_updated_filmworks_ids_by_state_name(self, ids: str, state_name: str) -> str:
        if state_name == 'person':
            SQL = f'''
            SELECT fm.id,
                   fm.updated_at
            FROM movies_filmwork fm
                LEFT JOIN movies_filmwork_actors a ON a.filmwork_id = fm.id
                LEFT JOIN movies_filmwork_writers w ON w.filmwork_id = fm.id
                LEFT JOIN movies_filmwork_directors d ON d.filmwork_id = fm.id
            WHERE a.person_id IN ({ids}) OR 
                  w.person_id IN ({ids}) OR 
                  d.person_id in ({ids})
            ORDER BY fm.updated_at
            '''
        else:
            SQL = f'''
            SELECT fm.id,
                   fm.updated_at
            FROM movies_filmwork fm
                LEFT JOIN movies_filmwork_genres g ON g.filmwork_id = fm.id
            WHERE g.genre_id IN ({ids})
            ORDER BY fm.updated_at
            '''
        return SQL

    def __get_updated_filmworks_by_ids_sql(self, filmworks_ids: str) -> str:
        SQL = f'''
        {self.__get_base_filmwork_sql()}
        WHERE fm.id in ({filmworks_ids})
        '''
        return SQL

    def __get_updated_filmworks_sql(self, updated_at: str) -> str:
        SQL = f'''
        {self.__get_base_filmwork_sql()}
        WHERE fm.updated_at > '{updated_at}'
        ORDER BY fm.updated_at
        LIMIT {self.chunk_size}
        '''
        return SQL

    def __get_base_filmwork_sql(self) -> str:
        with_actors_sql = self.__get_sql_for_m2m_person('movies_filmwork_actors')
        with_writers_sql = self.__get_sql_for_m2m_person('movies_filmwork_writers')
        with_directors_sql = self.__get_sql_for_m2m_person('movies_filmwork_directors')
        SQL = f'''
        WITH genres as (
            SELECT m.id, 
                   string_agg(g.title, ',') titles
            FROM movies_filmwork m
                LEFT JOIN movies_filmwork_genres mfg on m.id = mfg.filmwork_id 
                LEFT JOIN movies_genre g on mfg.genre_id = g.id
            GROUP BY m.id
        ),
        {with_actors_sql},
        {with_directors_sql},
        {with_writers_sql}

        SELECT 
               fm.updated_at,
               fm.id,
               fm.rating,
               genres.titles genres_titles,
               fm.title,
               fm.description,
               actors.ids actors_ids,
               actors.persons_names actors_names,
               writers.ids writers_ids,
               writers.persons_names writers_names,
               directors.persons_names directors_names
        FROM movies_filmwork fm
            LEFT JOIN genres ON genres.id = fm.id
            LEFT JOIN actors ON actors.id = fm.id
            LEFT JOIN writers ON writers.id = fm.id
            LEFT JOIN directors ON directors.id = fm.id
        '''
        return SQL

    @backoff()
    def get_db_connection(self):
        dsn_string = ' '.join([f'{key}={value}' for key, value in self.dsn.items()])
        pg_conn = psycopg2.connect(dsn=dsn_string, cursor_factory=RealDictCursor)
        return pg_conn

    @backoff()
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

    def get_updated_table_data(self, state_name: str) -> list[list, str]:
        pg_conn = self.get_db_connection()
        postgres_cur = pg_conn.cursor()
        state = self.redis_storage.retrieve_state(state_name)
        state = state if state else config('DEFAULT_UPDATED_AT_STATE')
        if state_name != self.redis_storage.states[0]:
            postgres_cur.execute(self.__get_updated_table_ids_sql(state, state_name))
        else:
            postgres_cur.execute(self.__get_updated_filmworks_sql(state))
        data = postgres_cur.fetchall()
        if data:
            updated_state_value = data[-1]['updated_at']
            self.redis_storage.save_state(state_name, str(updated_state_value))
            if state_name != self.redis_storage.states[0]:
                ids = ','.join([f"'{row['id']}'" for row in data])
                postgres_cur.execute(self.__get_updated_filmworks_ids_by_state_name(ids, state_name))
                updated_filmworks_ids_data = postgres_cur.fetchall()
                updated_filmworkds_ids = ','.join([f"'{filmwork['id']}'" for filmwork in updated_filmworks_ids_data])
                postgres_cur.execute(self.__get_updated_filmworks_by_ids_sql(updated_filmworkds_ids))
                data = postgres_cur.fetchall()
        return [data, state_name]

    @coroutine
    def extract(self, target):
        while True:
            sleep(5)
            updated_filmworks = [self.get_updated_table_data(self.redis_storage.states[2]),
                                 self.get_updated_table_data(self.redis_storage.states[1]),
                                 self.get_updated_table_data(self.redis_storage.states[0])]
            for data in updated_filmworks:
                if data[0]:
                    target.send(data[0])
                    logging.info(f'Found {len(data[0])} updated filmworks of table {data[1]}')

    @coroutine
    def transform(self, target):
        while True:
            filmwork_data = (yield)
            index_film_data = []
            for film in filmwork_data:
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
            target.send(index_film_data)

    @coroutine
    def load(self):
        while True:
            index_film_data = (yield)
            self.__load_to_es(index_film_data)
            logging.info(f"{len(index_film_data)} filmworks loaded to Elastic index")


if __name__ == '__main__':
    input_dsn = {'dbname': config('DB_NAME'),
                 'user': config('DB_USER'),
                 'password': config('DB_PASSWORD'),
                 'host': config('DB_HOST') or 'localhost',
                 'port': config('DB_PORT') or '5432'
                 }

    r = redis.Redis(host=config('REDIS_HOST'), port=config('REDIS_PORT'))
    input_redis_storage = RedisStorage(redis_adapter=r)
    etl = ETL(dsn=input_dsn,
              redis_storage=input_redis_storage,
              chunk_size=config('CHUNK_SIZE'),
              elastic_url=config('ELASTIC_URL'),
              index_name=config('ELASTIC_INDEX_NAME')
              )
    load_process = etl.load()
    transform_process = etl.transform(load_process)
    extract_process = etl.extract(target=transform_process)
    logging.info("")
