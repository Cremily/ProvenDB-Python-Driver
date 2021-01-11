from collections import UserDict
import datetime
from pyproven.proofs import GetDocumentProofResponse


from typing import Any, Optional, TypeVar, Union, Dict

from pymongo.database import Database as PymongoDatabase
from pymongo.errors import PyMongoError

from pyproven.history import DocumentHistoryResponse
from pyproven.exceptions import (BulkLoadException, CompactException, CreateIgnoredException, 
                                DocumentHistoryException, 
                                GetVersionException, 
                                ListVersionException, PrepareForgetException, 
                                SetVersionException)
from pyproven.versions import (GetVersionResponse,
                              SetVersionResponse,
                              CompactResponse,
                              ListVersionsResponse)

from pyproven.utilities import (BulkLoadKillResponse,
                               BulkLoadStartResponse,
                               BulkLoadStatusResponse,
                               BulkLoadStopResponse, CreateIgnoredResponse, ExecuteForgetResponse, PrepareForgetResponse,
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

    def compact_versions(self,start_version: int, end_version: int) -> CompactResponse:
        """Compacts all proofs, versions and documents in the db between two given versions,
        deleting all data that only exists between the two versions.

        :param start_version: The first version to compact from. 
        :type start_version: int
        :param end_version: The last version to compact to. 
        :type end_version: int
        :raises CompactException: pyproven exception when ProvenDB fails to compact between the two given versions. 
        :return: A dict-like object containing the number of deleted proofs, versions and documents.
        :rtype: CompactResponse
        """
        command_args = {"startVersion": start_version, "endVersion": end_version}
        try:
            response = self.db.command("compact",command_args)
            return CompactResponse(response)
        except PyMongoError as err:
            raise CompactException(
                f"Failed to compact on {self.db.name} between versions {start_version} and {end_version}.",err
                ) from None
    def create_ignored(self, collection: str) -> CreateIgnoredResponse:
        """Sets a collection to be ignored; it will  be identical among versions, not include metadata, 
        and not included in proofs.

        :param collection: The name of the collection to be ignored.
        :type collection: str
        :return: A dict-like object with the time of the operation.
        :raises CreateIgnoredException: pyproven exception when database fails to ignore the given collection. 
        :rtype: CreateIgnoredResponse
        """
        try:
            response = self.db.command("createIgnored",collection)
            return CreateIgnoredResponse(response)
        except PyMongoError as err:
            raise CreateIgnoredException(f"Failed to set collection {collection} on db {self.db.name} to be ignored.",err) from None
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
        :rtype: DocumentHistoryResponse
        """
        command_args = {"collection":collection,"filter":filter}
        if projection:
            command_args.update({"projection":projection})
        try:
            document = self._command_helper("docHistory",command_args)
            return DocumentHistoryResponse(document)
        except PyMongoError as err:
            raise DocumentHistoryException(f"""
                Failed to produce history for db {self.db.name} with args {command_args}""", err
                ) from None

    def forget_prepare(self, collection: str, filter: Dict[str,Any], 
                        min_version: Optional[int] = 0, max_version: Optional[int] = None,
                        inclusive_range: bool = True) -> PrepareForgetResponse:
        if not max_version:
            max_version = self.get_version()
        command_args: Dict[str,Any] = {
            "collection": collection, 
            "filter": filter, 
            "minVersion": min_version, 
            "maxVersion": max_version, 
            "inclusiveRange":inclusive_range}
        try:
            response = self.db.command("forget",{"prepare":command_args})
            return PrepareForgetResponse(response)
        except PyMongoError as err:
            raise PrepareForgetException(f"Failure to prepare a forget operation on db {self.db} with arguments {command_args}",err)
    
    def forget_execute(self,forget_id: int, password: str) -> ExecuteForgetResponse:
        command_args: Dict[str,Any] = {"forgetId":forget_id,"password":password}
        try:
            response = self.db.command("forget",{"execute":command_args})
            return ExecuteForgetResponse(response)
        except PyMongoError as err:
            raise PrepareForgetException(f"""Failure to execute a forget operation on db {self.db} 
                                        with password {password} and forget_id {forget_id}""".err)
    
    def get_document_proof(self, collection: str, filter: Dict[str,Any], 
                           version: int, proof_format: Optional[str] = "json"):
        command_args: Dict[str,Any] = {
            "collection": collection,
            "filter":filter,
            "version":version,
            "format":proof_format}
        try: 
            response = self.db.command("getDocumentProof",command_args)
            return GetDocumentProofResponse(response)
        except PyMongoError as err:
            raise GetDoc
        
    

    def get_version(self) -> GetVersionResponse:
        """Gets the version the db is set to. See https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document. 
        :raises GetVersionException: pyproven exception when db fails to return the current version.
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
        :raises SetVersionException: pyproven exception when db fails to set the given version. 
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
    
    

