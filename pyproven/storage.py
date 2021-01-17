from collections import UserDict
from typing import Any, Dict
from pyproven.response import ProvenResponse


class ListStorageResponse(ProvenResponse):
    """Dict-like object holding the list of 'collection: collection_storage-size' key-value pairs held by `:class:pyproven.database.ProvenDB.list_storage() """

    def __init__(self, document: Dict[str, Any]):
        super().__init__(document)
        self.storageList = [ListStorageDocuments(i) for i in self["storageList"]]


class ListStorageDocuments(UserDict):
    """Dict-like object holding the 'collection: collection_storage_size' key-value pair given by `:class:pyproven.database.ProvenDB.list_storage()"""