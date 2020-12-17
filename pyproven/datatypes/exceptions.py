from typing import Any, Dict

from pymongo.errors import PyMongoError

class PyProvenException(Exception):
    """Base class for all PyProven exceptions."""
    def __init__(self,
                message:str=None, pymongo_exception:PyMongoError=None,
                proven_err_doc:Dict[str,Any]=None) -> None:
        super().__init__(message)
        self.message = message
        self.mongo_excep = pymongo_exception
        self.err_doc = proven_err_doc
    def __repr__(self):
        explain = self.message + "\n"
        if self.mongo_excep:
            explain += f"pymongo raised the following exception {str(self.mongo_excep)}. \n"
        if self.err_doc:
            explain += f"ProvenDB returned the following error document {str(self.err_doc)}"
class VersionException(PyProvenException):
    """Base class for setVersion and getVersion exceptions."""
class VersionSetException(PyProvenException):
    """Exception raised when :class:`pyproven.database.database.ProvenDB`
    is given an invalid version argument."""
class VersionGetException(PyProvenException):
    """Exception raised when :class:pyproven.database.database.ProvenDB' is unable 
    to retrieve current version of database."""