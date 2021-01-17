from collections import UserDict
import datetime
from pyproven.storage import ListStorageResponse

from bson.son import SON
from pyproven.proofs import (
    GetDocumentProofResponse,
    GetVersionProofResponse,
    _process_document_proof,
)


from typing import Any, Optional, TypeVar, Union, Dict

from pymongo.database import Database as PymongoDatabase
from pymongo.errors import PyMongoError

from pyproven.history import DocumentHistoryResponse
from pyproven.exceptions import (
    BulkLoadAlreadyStartedException,
    BulkLoadException,
    BulkLoadNotStartedException,
    BulkLoadStartException,
    BulkLoadStopException,
    CompactException,
    CreateIgnoredException,
    DocumentHistoryException,
    GetDocumentProofException,
    GetVersionException,
    GetVersionProofException,
    ListStorageException,
    ListVersionException,
    PrepareForgetException,
    SetVersionException,
)
from pyproven.versions import (
    GetVersionResponse,
    SetVersionResponse,
    CompactResponse,
    ListVersionsResponse,
)

from pyproven.utilities import (
    BulkLoadKillResponse,
    BulkLoadStartResponse,
    BulkLoadStatusResponse,
    BulkLoadStopResponse,
    CreateIgnoredResponse,
    ExecuteForgetResponse,
    PrepareForgetResponse,
)
from pyproven.enums import BulkLoadEnums

from bson import BSON


def _fix_op_msg(
    flags, command, dbname, read_preference, slave_ok, check_keys, opts, ctx=None
):
    """Get a OP_MSG message."""
    import pymongo.message

    command["$db"] = dbname
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
                flags, command, identifier, docs, check_keys, opts, ctx
            )
        return pymongo.message._op_msg_uncompressed(
            flags, command, identifier, docs, check_keys, opts
        )
    finally:
        # Add the field back to the command.
        if identifier:
            command[identifier] = docs


class ProvenDB:
    """Proven DB Database object that wraps the original pymongo Database object. """

    def __init__(self, database: PymongoDatabase, *args, **kwargs):
        """Constructor method"""
        self.db: PymongoDatabase = database
        # hack to temp fix issue between pymongo and provendb instances.
        # TODO remove once fix is pushed to production provendbs.
        try:
            if kwargs["provendb_hack"]:
                import pymongo.message

                pymongo.message._op_msg = _fix_op_msg
        except:
            pass

    def __getattr__(self, name: str) -> Any:
        """Calls :class:`pymongo.database.Database` object attribute or method when none could be found in self.

        :param name: Name of :class:`pymongo.database.Database` attribute or method to be called.
        :type name: str
        :return: Value from :class:`pymongo.database.Database` attribute or method.
        :rtype: Any
        """
        return getattr(self.db, name)

    def __getitem__(self, name: str) -> Any:
        return getattr(self.db, name)

    def bulk_load_start(self) -> BulkLoadStartResponse:
        """Starts a bulk load on the database. Bulk loads allow multiple inserts without incrementing the version.

        :raises BulkLoadException: A PyProven exception raised when db fails to start bulk loading, usually because the db is already bulk loading.
        :return: A dict-like object that holds the current version.
        :rtype: BulkLoadStartResponse
        """
        if self.bulk_load_status().status == "on":
            raise BulkLoadAlreadyStartedException(
                f"Attemped to start bulk load on db {self.db.name}, but the db is already bulk loading."
            )
        try:
            document: Dict[str, Any] = self.db.command(
                "bulkLoad", BulkLoadEnums.START.value
            )
            return BulkLoadStartResponse(document)
        except PyMongoError as err:
            raise BulkLoadStartException(
                f"Failure to start bulk load on db {self.db.name}", err
            ) from None

    def bulk_load_stop(self) -> BulkLoadStopResponse:
        """Stops a bulk load on a database, failing if there is any outstanding operations.

        :raises BulkLoadException: A PyProven exception when failing to stop a bulk load.
        :return: [description]
        :rtype: BulkLoadStopResponse
        """
        if self.bulk_load_status().status == "off":
            raise BulkLoadNotStartedException(
                "Attempted to stop a bulk load on db {self.db.name}, but the db isn't currently bulk loading."
            )
        try:
            document: Dict[str, Any] = self.db.command(
                "bulkLoad", BulkLoadEnums.STOP.value
            )
            return BulkLoadStopResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to stop bulk load on db {self.db.name}", err
            ) from None

    def bulk_load_kill(self) -> BulkLoadKillResponse:
        if self.bulk_load_status().status == "off":
            raise BulkLoadNotStartedException(
                "Attempted to kill a bulk load on db {self.db.name}, but the db isn't currently bulk loading."
            )
        try:
            document: Dict[str, Any] = self.db.command(
                "bulkLoad", BulkLoadEnums.KILL.value
            )
            return BulkLoadKillResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to start bulk load on db {self.db.name}", err
            ) from None

    def bulk_load_status(self) -> BulkLoadStatusResponse:
        try:
            document: Dict[str, Any] = self.db.command(
                "bulkLoad", BulkLoadEnums.STATUS.value
            )
            return BulkLoadStatusResponse(document)
        except PyMongoError as err:
            raise BulkLoadException(
                f"Failure to start bulk load on db {self.db.name}", err
            ) from None

    def compact_versions(self, start_version: int, end_version: int) -> CompactResponse:
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
            response = self.db.command("compact", command_args)
            return CompactResponse(response)
        except PyMongoError as err:
            raise CompactException(
                f"Failed to compact on {self.db.name} between versions {start_version} and {end_version}.",
                err,
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
            response = self.db.command("createIgnored", collection)
            return CreateIgnoredResponse(response)
        except PyMongoError as err:
            raise CreateIgnoredException(
                f"Failed to set collection {collection} on db {self.db.name} to be ignored.",
                err,
            ) from None

    def doc_history(
        self, collection: str, filter: Dict[str, Any], projection: Dict[str, Any] = None
    ) -> DocumentHistoryResponse:
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
        command_args = {"collection": collection, "filter": filter}
        if projection:
            command_args.update({"projection": projection})
        try:
            document = self.db.command("docHistory", command_args)
            return DocumentHistoryResponse(document)
        except PyMongoError as err:
            raise DocumentHistoryException(
                f"""
                Failed to produce history for db {self.db.name} with args {command_args}""",
                err,
            ) from None

    def forget_prepare(
        self,
        collection: str,
        filter: Dict[str, Any],
        min_version: Optional[int] = 0,
        max_version: Optional[int] = None,
        inclusive_range: bool = True,
    ) -> PrepareForgetResponse:
        if not max_version:
            max_version = self.get_version()
        command_args: Dict[str, Any] = {
            "collection": collection,
            "filter": filter,
            "minVersion": min_version,
            "maxVersion": max_version,
            "inclusiveRange": inclusive_range,
        }
        try:
            response = self.db.command("forget", {"prepare": command_args})
            return PrepareForgetResponse(response)
        except PyMongoError as err:
            raise PrepareForgetException(
                f"Failure to prepare a forget operation on db {self.db} with arguments {command_args}",
                err,
            ) from None

    def forget_execute(self, forget_id: int, password: str) -> ExecuteForgetResponse:
        command_args: Dict[str, Any] = {"forgetId": forget_id, "password": password}
        try:
            response = self.db.command("forget", {"execute": command_args})
            return ExecuteForgetResponse(response)
        except PyMongoError as err:
            raise PrepareForgetException(
                f"""Failure to execute a forget operation on db {self.db} 
                                        with password {password} and forget_id {forget_id}""".err
            ) from None

    def get_document_proof(
        self,
        collection: str,
        filter: Dict[str, Any],
        version: int,
        proof_format: Optional[str] = "json",
    ):
        command_args: Dict[str, Any] = {
            "collection": collection,
            "filter": filter,
            "version": version,
            "format": proof_format,
        }
        try:
            response = self.db.command("getDocumentProof", command_args)
            return GetDocumentProofResponse(response)
        except PyMongoError as err:
            raise GetDocumentProofException(
                f"""Failed to get proof for documents on {self.db.name} with arguments {command_args}""",
                err,
            ) from None

    def get_version(self) -> GetVersionResponse:
        """Gets the version the db is set to. See https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document.
        :raises GetVersionException: pyproven exception when db fails to return the current version.
        :rtype: GetVersionData
        """
        try:
            document = self.db.command("getVersion", 1)
            return GetVersionResponse(document)
        except PyMongoError as err:
            raise GetVersionException(
                f"Failure to get version from db {self.db.name}.", pymongo_exception=err
            ) from None

    def get_version_proof(
        self,
        proof_id: Union[str, int],
        proof_format: str = "binary",
        list_collections: bool = False,
    ) -> GetVersionProofResponse:
        command_args: SON = SON({"getProof": proof_id})
        command_args.update({"format": proof_format})
        command_args.update({"listCollections": list_collections})
        try:
            document = self.db.command(command_args)
            return GetVersionProofResponse(document)
        except PyMongoError as err:
            raise GetVersionProofException(
                f"Failed to get the given version for proof_id {proof_id} on db {self.db.name}",
                err,
            )

    def list_storage(self) -> ListStorageResponse:
        """Fetches the storage size for each collection in the db.

        :return: A dict-like object holding a list of dict-like objects,
        each containg a single 'collection_name: collection_storage_size' key-value pair.
        :rtype: ListStorageResponse
        """
        try:
            response = self.db.command("listStorage")
            return ListStorageResponse(response)
        except PyMongoError as err:
            raise ListStorageException(
                f"Failed to list storage sizes for db {self.db.name}", err
            )

    def list_versions(
        self,
        start_date: datetime.datetime = datetime.datetime.today()
        - datetime.timedelta(days=1),
        end_date: datetime.datetime = datetime.datetime.today(),
        limit: int = 10,
        sort_direction: int = -1,
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
        command_args: Dict[str, Union[datetime.datetime, int]] = {
            "startDate": start_date,
            "endDate": end_date,
            "limit": limit,
            "sortDirection": sort_direction,
        }
        try:
            document: Dict[str, Any] = self.db.command({"listVersions": command_args})
            return ListVersionsResponse(document)
        except PyMongoError as err:
            raise ListVersionException(
                f"Unable to get version list from {self.db.name} with arguments {command_args}.",
                err,
            ) from None

    def set_version(
        self, date: Union[str, int, datetime.datetime]
    ) -> SetVersionResponse:
        """Sets the database version to a given version identifier.

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :raises SetVersionException: pyproven exception when db fails to set the given version.
        :return: A dict-like object representing the provenDB return document.
        :rtype: SetVersionData
        """
        try:
            document = self.db.command("setVersion", date)
            return SetVersionResponse(document)
        except PyMongoError as e:
            raise SetVersionException(
                f"Failure to set version {date} on db {self.db.name}"
                + f"with date {date}.",
                e,
            ) from None
