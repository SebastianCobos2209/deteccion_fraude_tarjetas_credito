"""
infrastructure/null_document_repository.py
"""
from __future__ import annotations

from typing import List

from infrastructure.contracts.document_repository import DocumentRepository


class NullDocumentRepository(DocumentRepository):

    def save(self, collection: str, documents: List[dict]) -> None:
        """No hace nada. MongoDB está desactivado."""
        pass