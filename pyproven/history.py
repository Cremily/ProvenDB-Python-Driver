from collections import UserDict
from typing import Any, Dict, List
from pyproven.response import ProvenResponse


class DocumentHistoryResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["history"] = [DocumentHistoryItem(history) for history in self["history"]]
        self.history = self["history"]
        self.collection = self["collection"]


class DocumentHistoryItem(UserDict):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["versions"] = [
            DocumentHistoryVersion(version) for version in self["versions"]
        ]
        self._id = self["_id"]
        self.versions = self["versions"]


class DocumentHistoryVersion(UserDict):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.minVersion: float = self["minVersion"]
        self.maxVersion: float = self["maxVersion"]
        self.status: str = self["status"]
        self.started: str = self["started"]
        self.ended: str = self["ended"]
        self.document: Dict[str, Any] = self["document"]
