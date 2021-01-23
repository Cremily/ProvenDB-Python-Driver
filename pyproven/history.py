from collections import UserDict
from typing import Any, Dict, List
from pyproven.response import ProvenResponse


class DocumentHistoryResponse(ProvenResponse):
    """A dict-like object holding a list of documents, each holding their version history."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["history"] = [DocumentHistoryItem(history) for history in self["history"]]
        self.history = self["history"]
        self.collection = self["collection"]


class DocumentHistoryItem(UserDict):
    """A dict like object that represents a specific document in the DB, and contains each version that was searched for."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["versions"] = [
            DocumentHistoryVersion(version) for version in self["versions"]
        ]
        self._id = self["_id"]
        self.versions = self["versions"]


class DocumentHistoryVersion(UserDict):
    """A dict-like object that represents an individual history of a specific document."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.minVersion: float = self["minVersion"]
        self.maxVersion: float = self["maxVersion"]
        self.status: str = self["status"]
        self.started: str = self["started"]
        self.ended: str = self["ended"]
        self.document: Dict[str, Any] = self["document"]
