from collections import UserDict
import datetime


from typing import Any, Optional, TypeVar, Union, Dict

from pymongo.database import Database as PymongoDatabase
from pymongo.errors import PyMongoError

from pyproven.history import DocumentHistoryResponse
from pyproven.exceptions import BulkLoadException, DocumentHistoryException, GetVersionException, ListVersionException, SetVersionException
from pyproven.versions import (GetVersionResponse,
                              SetVersionResponse,
                              CompactResponse,
                              ListVersionsResponse)

from pyproven.utilities import (BulkLoadKillResponse,
                               BulkLoadStartResponse,
                               BulkLoadStatusResponse,
                               BulkLoadStopResponse,
                               )
from pyproven.enums import BulkLoadEnums
def _fix_op_msg(flags, command, dbname, read_preference, slave_ok, check_keys,
            opts, ctx=None):
    """Get a OP_MSG message."""
    import pymongo.message
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
    """Proven DB Database object that wraps the original pymongo Database object. """
    def __init__(self,database: PymongoDatabase,*args,**kwargs):
        """Constructor method"""
        self.db: PymongoDatabase = database
        #hack to temp fix issue between pymongo and provendb instances.
        #TODO remove once fix is pushed to production provendbs. 
        try:
            if kwargs['provendb_hack']:
                import pymongo.message
                pymongo.message._op_msg = _fix_op_msg
        except:
            pass
        
    
    def __getattr__(self,name: str) -> Any:
        """Calls :class:`pymongo.database.Database` object attribute or method when none could be found in self.

        :param name: Name of :class:`pymongo.database.Database` attribute or method to be called.
        :type name: str
        :return: Value from :class:`pymongo.database.Database` attribute or method.
        :rtype: Any
        """
        return getattr(self.db,name)
    def __getitem__(self,name:str) -> Any:
        return getattr(self.db,name)
    
    def bulk_load_start(self) -> BulkLoadStartResponse:
        try:
            document: Dict[str,Any] = self.db.command("bulkLoad",BulkLoadEnums.START)
            return BulkLoadStartResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to start bulk load on db {self.db.name}",err)  from None
    def bulk_load_stop(self) -> BulkLoadStopResponse:
        try:
            document: Dict[str,Any] = self.db.command("bulkLoad",BulkLoadEnums.STOP)
            return BulkLoadStartResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to stop bulk load on db {self.db.name}",err)  from None
    def bulk_load_kill(self) -> BulkLoadKillResponse:
        try:
            document: Dict[str,Any] = self.db.command("bulkLoad",BulkLoadEnums.KILL)
            return BulkLoadStartResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to start bulk load on db {self.db.name}",err)  from None
    def bulk_load_status(self) -> BulkLoadStatusResponse:
        try:
            document: Dict[str,Any] = self.db.command("bulkLoad",BulkLoadEnums.START)
            return BulkLoadStartResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to start bulk load on db {self.db.name}", err)  from None

    def doc_history(self,collection:str, filter :Dict[str,Any], 
        projection :Dict[str,Any] = None) -> DocumentHistoryResponse:
        """Returns the document history of a filtered collection. 
            See https://provendb.readme.io/docs/dochistory

        :param collection: Name of collection to find history.
        :type collection: str
        :param filter: MongoDB document filter to search for specific documents in the collection.
        :type filter: Dict[str,Any]
        :param projection: A projection document that specifies fields to retrieve from documents.
                           defaults to returning all fields.
        :type projection: Dict[str,Any], optional
        :raises DocumentHistoryException: pyproven exception when ProvenDB fails to retrieve 
                                          the given document history. 
        :return: A dict-like object representing the ProvenDB return document.
                 See :class:`pyproven.history.DocumentHistoryResponse`. 
        :rtype: DocumentHistoryResponse
        """
        command = {"collection":collection,"filter":filter}
        if projection:
            command.update({"projection":projection})
        try:
            document = self._command_helper("docHistory",command)
            return DocumentHistoryResponse(document)
        except PyMongoError as err:
            raise DocumentHistoryException(
            self.db.name,command,err) from None

    
    

    def get_version(self) -> GetVersionResponse:
        """Gets the version the db is set to. See https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :rtype: GetVersionData
        """
        try:
            document = self.db.command("getVersion",1)
            return GetVersionResponse(document)
        except PyMongoError as err:
            raise GetVersionException(
            f"Failure to get version from db {self.db.name}.",pymongo_exception=err
            ) from None
    
    def set_version(self,
        date: Union[str,int,datetime.datetime]) -> SetVersionResponse:
        """ Sets the database version to a given version identifier. 

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :return: A dict-like object representing the provenDB return document.
        :rtype: SetVersionData
        """
        try:
            document = self.db.command("setVersion",date)
            return SetVersionResponse(document)
        except PyMongoError as e:
            raise SetVersionException(f"Failure to set version {date} on db {self.db.name}" +
                f"with date {date}.",e) from None
    
    
    def list_versions(self,
        start_date: Optional[datetime.datetime] = datetime.datetime.today() - datetime.timedelta(days=1),
        end_date: Optional[datetime.datetime] = datetime.datetime.today(),
        limit: Optional[int] = 10,
        sort_direction: Optional[int] = -1
        ) -> ListVersionsResponse:
        """Retrieves a list of versions given a search parameter. 
        :param start_date: Specifies first date to retrieve versions, defaults to 24 hours from now
        :type start_date: Optional[datetime.datetime]
        :param end_date: Last date to retrieve documents, defaults to now
        :type end_date: Optional[datetime.datetime]
        :param limit: Number of version documents to retrieve, defaults to 10
        :type limit: Optional[int], optional
        :param sort_direction: -1 to retrieve versions in descending order, 1 ascending order. Defaults to -1.
        :type sort_direction: Optional[int], optional
        :raises ListVersionException: pyproven exception when fails to retrieve version list.
        :return: A dict-like object representing the ProvenDB response document. 
        :rtype: ListVersionsResponse
        """
        command_args: Dict[str,Union[datetime.datetime,int]] = {
            "startDate":start_date,
            "endDate":end_date,
            "limit":limit,
            "sortDirection":sort_direction
        }
        try:
            document: Dict[str,Any] = self.db.command({"listVersions":command_args})
            return ListVersionsResponse(document)
        except PyMongoError as err:
            raise ListVersionException(
        f"Unable to get version list from {self.db.name} with arguments {command_args}."
        ,err) from None
    
    

