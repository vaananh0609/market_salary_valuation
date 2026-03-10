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
RAW_COLLECTION.create_index("link", unique=True)


def get_raw_collection() -> Collection:
    """Return the raw jobs collection with deduplication index already configured."""
    return RAW_COLLECTION
