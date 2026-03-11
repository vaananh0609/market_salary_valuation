"""Load environment settings and expose the MongoDB raw collection."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


def _enforce_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"{name} must be set in environment")
    return value


MONGO_URI = _enforce_env("MONGO_URI")
RAW_DB_NAME = _enforce_env("RAW_DB_NAME")
RAW_COLLECTION_NAME = _enforce_env("RAW_COLLECTION_NAME")

CLIENT = MongoClient(MONGO_URI)
RAW_DB = CLIENT[RAW_DB_NAME]
RAW_COLLECTION = RAW_DB[RAW_COLLECTION_NAME]


def get_mongo_client() -> MongoClient:
    """Return the singleton Mongo client created from environment settings."""
    return CLIENT


def ensure_raw_collection_index(
    collection: Collection | None = None, *, field_name: str = "link"
) -> str:
    """Create the unique index for the supplied collection (defaults to the raw collection)."""

    target = collection if collection is not None else RAW_COLLECTION
    return target.create_index(field_name, unique=True)


def get_raw_collection(client: MongoClient | None = None) -> Collection:
    """Return the raw jobs collection, ensuring the dedupe field is indexed."""

    active_client = client or CLIENT
    db = active_client[RAW_DB_NAME]
    collection = db[RAW_COLLECTION_NAME]
    ensure_raw_collection_index(collection)
    return collection
