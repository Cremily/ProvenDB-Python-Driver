from collections import UserDict
from datetime import datetime
from typing import Any, Dict
class DocumentHistory(UserDict):
    def __init__(self,document:Dict[str,Any]):
        super().__init__(document)
        self.min_version:int = document['minVersion']
        self.max_version:int = document['maxVersion']
        self.status: str = document['status']
        self.started: datetime = document['started']
        self.ended: datetime = document['ended']
        self.document: Dict[str,Any] = document['document']
class DocumentHistoryResponse(UserDict):
    def __init__(self,document:Dict[str,Any]):
        self.histories = [DocumentHistory(doc) for doc in document['docHistory']]
        super().__init__(self.histories)