from typing import Any
from pymongo import AsyncMongoClient

mongo_db_connection = AsyncMongoClient[Any]("localhost", 27017)
