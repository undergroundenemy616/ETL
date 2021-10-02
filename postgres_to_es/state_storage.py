import abc

from redis import Redis


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