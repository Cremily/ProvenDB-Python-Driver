from collections import UserDict, UserList

from typing import Any, Dict, Optional, Union,List

from datetime import datetime

class CompactResponse(UserDict):
    #TODO
    pass


class ListVersionDocument(UserDict):
    def __init__(self,document:Dict[str,any]):
        super().__init__(document)
        self.version = document['version']
        self.status = document['status']
        self.effective_date = document['effectiveDate']
class ListVersionsResponse(UserDict):
    def __init__(self,document:Dict[str,Any]):
        self.ok = document['ok']
        self.versions = [ListVersionDocument(doc) for doc in document['versions']]
        super().__init__({'versions':self.versions})
        

class VersionResponse(UserDict):
    def __init__(self,document:Dict[str,Any]):
        super().__init__(document)
        self.response: str = document['response']
        self.version: float = document['version']
        self.status: str = document['status']
class GetVersionResponse(VersionResponse):
    pass
class SetVersionResponse(VersionResponse):
    pass

        