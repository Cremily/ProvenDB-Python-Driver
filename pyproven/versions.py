from collections import UserDict, UserList

from typing import Any, Dict, Optional, Union, List

from datetime import datetime

from pyproven.response import ProvenResponse


class CompactResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.nProofsDeleted = self["nProofsDeleted"]
        self.nVersionsDeleted = self["nVersionsDeleted"]


class ListVersionDocument(UserDict):
    """A dict-like object holding the individual version data. Data can be accessed via dict methods
    or by attribute."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.version: float = document["version"]
        self.status: str = document["status"]
        self.effective_date: str = document["effectiveDate"]


class ListVersionsResponse(UserDict):
    """A dict-like object containing the retrieved version data."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self["versions"] = [ListVersionDocument(doc) for doc in document["versions"]]


class VersionResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.response: str = document["response"]
        self.version: float = document["version"]
        self.status: str = document["status"]


class GetVersionResponse(VersionResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)


class SetVersionResponse(VersionResponse):
    pass
