import datetime
from pyproven.history import DocumentHistoryResponse

from typing import Any, Optional, Set, Union, Dict

import pymongo
from pymongo.errors import OperationFailure, PyMongoError
import pymongo.message
import pymongo.database

from pyproven.versions import GetVersionResponse, ListVersionsResponse
from pyproven.versions import SetVersionResponse,CompactResponse
from pyproven.exceptions import DocumentHistoryException, PyProvenException, SetVersionException,ListVersionException,GetVersionException
from pyproven.bulkload import BulkLoadData

def _fix_op_msg(flags, command, dbname, read_preference, slave_ok, check_keys,
            opts, ctx=None):
    """Get a OP_MSG message."""
    command['$db'] = dbname
    name = next(iter(command))
    try:
        identifier = pymongo.message._FIELD_MAP.get(name)
        docs = command.pop(identifier)
    except KeyError:
        identifier = ""
        docs = None
    try:
        if ctx:
            return pymongo.message._op_msg_compressed(
                flags, command, identifier, docs, check_keys, opts, ctx)
        return pymongo.message._op_msg_uncompressed(
            flags, command, identifier, docs, check_keys, opts)
    finally:
        # Add the field back to the command.
        if identifier:
            command[identifier] = docs
class ProvenDB():
    """Proven DB Database object that wraps the original pymongo Database object. 
    See :class:`database.ProvenDB` """
    
    def __init__(self,database: pymongo.database.Database,*args,**kwargs):
        """Constructor method"""
        self.db: pymongo.database.Database = database
        #hack to temp fix issue between pymongo and provendb instances.
        if kwargs['provendb_hack']:
            pymongo.message._op_msg = _fix_op_msg
        
    
    def __getattr__(self,name: str) -> Any:
        """Calls :class:`pymongo.database.Database` object attribute or method when none could be found in self.

        :param name: Name of :class:`pymongo.database.Database` attribute or method to be called.
        :type name: str
        :return: Value from :class:`pymongo.database.Database` attribute or method.
        :rtype: Any
        """
        return getattr(self.db,name)
    def doc_history(self,collection:str, filter :Dict[str,Any], 
        projection :Dict[str,Any] = None) -> DocumentHistoryResponse:
        command = {"collection":collection,"filter":filter}
        if projection:
            command.update({"projection":projection})
        try:
            document = self.db.command("docHistory",command)
            if document['ok']:
                return DocumentHistoryResponse(document)
        except PyMongoError as e:
            raise DocumentHistoryException(f"Unable to get document history on db {self.db.name}, with arguments {command}.",e) from None
    def get_version(self) -> GetVersionResponse:
        """Gets the version the db is set to.
        :see https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :rtype: GetVersionData
        """
        try:
            document = self.db.command("getVersion",1)
            if document['ok']:
                return GetVersionResponse(document)
            else:
                raise GetVersionException(proven_err_doc=document)
        except PyMongoError as e:
            raise GetVersionException(f"Failure to get version from db {self.db.name}.",e) from None
    
    def set_version(self,date: Union[str,int,datetime.datetime]) -> SetVersionResponse:
        """

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :return: A dict-like object representing the provenDB return document. 
        :rtype: SetVersionData
        """
        try:
            document = self.db.command("setVersion",date)
            return SetVersionResponse(document)
        except PyMongoError as e:
            raise SetVersionException(f"Failure to set version {date} on db {self.db.name}.",e) from None
    
    
    def list_versions(self,
        start_date: Optional[datetime.datetime] = datetime.datetime.today() - datetime.timedelta(days=1),
        end_date: Optional[datetime.datetime] = datetime.datetime.today(),
        limit: Optional[int] = 10,
        sort_direction: Optional[int] = -1
        ) -> ListVersionsResponse:
        """[summary]

        :param start_date: Earliest time as :class:`datetime.datetime` object. Defaults to 24 hours prior to the current date. 
        :type start_date: Optional[datetime], optional
        :param end_date: Latest time to retrieve version as :class:`datetime.datetime` object. Defaults to current date.
        :type end_date: Optional[datetime], optional
        :param limit:  Number of versions to retrieve. Defaults to 10.
        :type limit: Optional[int], optional
        :param sort_direction: 1 for results ascending chronologically, -1 descending. Defaults to -1. 
        :type sort_direction: Optional[int], optional
        :return: A list-like object holding all retrieved versions of database, containing verison documents as `pyproven.versions.ListVersionsSingularData`.
        :rtype: ListVersionsDocument
        """
        command_args: Dict[str,Union[datetime.datetime,int]] = {}
        if start_date:
            command_args.update({"startDate":start_date})
        if end_date:
            command_args.update({"endDate":end_date})
        if limit:
            command_args.update({"limit":limit})
        if sort_direction:
            command_args.update({"sortDirection":sort_direction})
        try:
            document: Dict[str,Any] = self.db.command({"listVersions":command_args})
            if document['ok']:
                return ListVersionsResponse(document)
            else:
                raise ListVersionException(f"Unable to get version list from {self.db.name} with arguments {command_args}",proven_error=document)
        except PyMongoError as e:
            raise ListVersionException(f"Unable to get version list from {self.db.name} with arguments {command_args}.",e) from None
    
    

