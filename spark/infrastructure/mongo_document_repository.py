"""
infrastructure/mongo_document_repository.py
"""
from __future__ import annotations

from typing import List

from pymongo import MongoClient

from config.settings import MONGO_URI, MONGO_DB
from infrastructure.contracts.document_repository import DocumentRepository
from utils.logger import get_logger

logger = get_logger("MongoDocumentRepository")


class MongoDocumentRepository(DocumentRepository):
    def __init__(
        self,
        mongo_uri: str = MONGO_URI,
        mongo_db:  str = MONGO_DB,
        timeout_ms: int = 5000,
    ) -> None:
        self._mongo_uri  = mongo_uri
        self._mongo_db   = mongo_db
        self._timeout_ms = timeout_ms

    def save(self, collection: str, documents: List[dict]) -> None:
        """
        Args:
            collection: nombre de la colección
            documents:  lista de dicts a insertar
        """
        if not documents:
            return
        try:
            client = MongoClient(
                self._mongo_uri,
                serverSelectionTimeoutMS=self._timeout_ms,
            )
            client[self._mongo_db][collection].insert_many(
                documents,
                ordered=False,
            )
            logger.info(
                f"{len(documents)} documentos insertados en '{collection}'"
            )
        except Exception as e:
            logger.error(f"Error insertando en '{collection}': {e}")
        finally:
            try:
                client.close()
            except Exception:
                pass