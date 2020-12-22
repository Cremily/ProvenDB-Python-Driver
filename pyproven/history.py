from collections import UserDict, UserList
from datetime import datetime
from typing import Any, Dict, List
class DocumentHistoryResponse(UserDict):
    def __init__(self, document: Dict[str,Any]):
        print(document)
        super().__init__(document)
        self['history'] = [DocumentHistoryItem(history) for history in self['history']]
        self.history = self['history']
        self.collection = self['collection']
class DocumentHistoryItem(UserDict):
    def __init__(self,document: Dict[str,Any]):
        super().__init__(document)
        self['versions'] = [DocumentHistoryVersion(version) for version in self['versions']]
        self._id = self['_id']
        self.versions = self['versions']
class DocumentHistoryVersion(UserDict):
    def __init__(self,document: Dict[str,Any]):
        super().__init__(document)
        print(type(document['ended']))