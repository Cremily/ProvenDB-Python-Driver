from collections import UserDict, UserList

from typing import Any, Dict, Optional, Union,List

from datetime import datetime

class VersionData(UserDict):
    def __init__(self,document:Dict[str,Any]):
        super().__init__(document)
        self.response: str = document['response']
        self.version: float = document['version']
        self.status: str = document['status']
class GetVersionData(VersionData):
    pass
class SetVersionData(VersionData):
    pass
class ListVersionsSingularData(UserDict):
    def __init__(self,document:Dict[str,any]):
        super().__init__(document)
        self.version = document['version']
        self.status = document['status']
        self.effective_date = document['effectiveDate']
class ListVersionsData(UserDict):
    def __init__(self,versions:List[Dict[str,Any]]):
        self.versions = [ListVersionsSingularData(doc) for doc in versions]
        super().__init__({'versions':self.versions})
        
        
        