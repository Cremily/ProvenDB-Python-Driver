from typing import Any, Dict, List
from collections import UserDict
from pyproven.response import ProvenResponse


class BulkLoadResponse(ProvenResponse):
    """ABC for bulk load response classes."""


class BulkLoadStartResponse(BulkLoadResponse):
    """ProvenDB response document to a command to start the bulk load."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.version = self["version"]


class BulkLoadStopResponse(BulkLoadResponse):
    """ProvenDB response document to a command to stop the bulk load. """


class BulkLoadKillResponse(BulkLoadResponse):
    """ProvenDB response document when a command to stop the bulk load,
    regardless of remaining operations."""


class BulkLoadStatusResponse(BulkLoadResponse):
    """ProvenDB response document that gives the current bulk load status."""

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.status = self["status"]


class CreateIgnoredResponse(ProvenResponse):
    """ProvenDB response document when setting a collection to be ignored."""

    class ClusterTime(UserDict):
        def __init__(self, document: Dict[str, Any]):
            super().__init__(document)
            self.clusterTime = document["clusterTime"]
            self.signature = self.Signature(document["signature"])

    class Signature(UserDict):
        def __init__(self, document: Dict[str, Any]):
            super().__init__(document)
            self.hash: bytes = self["hash"]
            self.keyId = self["keyId"]

    def __init__(self, document: Dict[str, Any]):
        # TODO: Contact ProvenDB team for updated docs.
        super().__init__(document)
        self.clusterTime = self.ClusterTime(document["$clusterTime"])
        self.operationTime = self["operationTime"]


class PrepareForgetSummary(UserDict):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.documentsToBeForgotten: int = self["documentsToBeForgotten"]
        self.uniqueDocuments: int = self["uniqueDocuments"]


class ExecuteForgetSummary(UserDict):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.documentsForgotten: int = self["documentsForgotten"]
        self.uniqueDocuments: int = self["uniqueDocuments"]


class PrepareForgetResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.forgetId: float = self["forgetId"]
        self.password: str = self["password"]
        self.forgetSummary: PrepareForgetSummary = PrepareForgetSummary(
            self["forgetSummary"]
        )


class ExecuteForgetResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.status: str = document["status"]
        self.forgetSummary: ExecuteForgetSummary = ExecuteForgetSummary(
            document["forgetSummary"]
        )

class RollbackResponse(ProvenResponse):
    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.version: List[RollbackVersion] = [RollbackVersion(i) for i in self['version']]

class RollbackVersion(UserDict):
    """Dict-like object holding the 'db_name: db_version' key-value pair given by `:class:pyproven.database.ProvenDB.rollback()`"""