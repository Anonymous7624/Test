"""Compatibility shim: MongoDB is the primary store (see `app.mongodb`)."""

from app.mongodb import close_mongo_client, get_database, get_db

__all__ = ["close_mongo_client", "get_database", "get_db"]
