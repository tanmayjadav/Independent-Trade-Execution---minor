from pymongo import MongoClient
from pymongo.errors import PyMongoError


class MongoDBClient:
    _client = None

    @classmethod
    def get_client(cls, uri: str):
        if cls._client is None:
            cls._client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        return cls._client
