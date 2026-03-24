"""
MongoDB connection and indexes. Uses the official PyMongo driver (sync), suitable for FastAPI route handlers.
"""

from collections.abc import Generator

from pymongo import MongoClient, ReturnDocument
from pymongo.database import Database

from app.config import settings

_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(
            settings.mongodb_uri,
            serverSelectionTimeoutMS=10_000,
        )
    return _client


def get_database() -> Database:
    return get_mongo_client()[settings.mongodb_database]


def close_mongo_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


def get_db() -> Generator[Database, None, None]:
    db = get_database()
    try:
        yield db
    finally:
        pass


def ensure_indexes(db: Database) -> None:
    """Idempotent index creation on startup."""
    db["counters"].create_index("_id", unique=True)
    db["users"].create_index("id", unique=True)
    db["users"].create_index("username", unique=True)
    db["user_settings"].create_index("user_id", unique=True)
    db["user_settings"].create_index("telegram_verify_code", sparse=True)
    db["listings"].create_index("id", unique=True)
    db["listings"].create_index([("user_id", 1), ("found_at", -1)])
    db["listings"].create_index(
        [("user_id", 1), ("source_url", 1)],
        unique=True,
        name="user_id_source_url_unique",
    )


def next_sequence(db: Database, name: str) -> int:
    """Monotonic integer ids (users, listings)."""
    doc = db["counters"].find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(doc["seq"])
