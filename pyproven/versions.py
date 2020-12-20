from collections import UserDict, UserList
from typing import Any, Dict, Optional, Union,List
from datetime import datetime
import pyproven.exceptions as proven_exceptions
import pymongo.database
class VersionDocument(UserDict):
    def __init__(self,document:Dict[str,Any]):
        super().__init__(document)
        self.response: str = document['response']
        self.version: float = document['version']
        self.status: str = document['status']
class ListVersionsDocument(UserList):
    def __init__(self,documents:List[Dict[str,Any]]):
        super().__init__([VersionDocument(doc) for doc in documents])
        