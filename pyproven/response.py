from typing import Any, Dict
from collections import UserDict


class ProvenDocument(UserDict):
    """ABC for all ProvenDB data documents."""


class ProvenResponse(ProvenDocument):
    """ABC for ProvenDB response objects."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.ok = document["ok"]
