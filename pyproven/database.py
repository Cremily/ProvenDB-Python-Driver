from collections import UserDict
import datetime

from pymongo.collection import Collection
from pyproven import exceptions
from pyproven.storage import ListStorageResponse

from bson.son import SON
from pyproven.proofs import (
    GetDocumentProofResponse,
    GetVersionProofResponse,
    SubmitProofResponse,
    VerifyProofResponse,
    _process_document_proof,
)


from typing import Any, List, Optional, TypeVar, Union, Dict

from pymongo.database import Database as PymongoDatabase
from pymongo.errors import PyMongoError

from pyproven.history import DocumentHistoryResponse
from pyproven.exceptions import (
    BulkLoadAlreadyStartedError,
    CompactProofError,
    CompactValueError,
    extract_error_info,
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
    HideMetadataResponse,
    PrepareForgetResponse,
    RollbackResponse,
    ShowMetadataResponse,
)
from pyproven.enums import BulkLoadEnums

from bson import BSON


def _fix_op_msg(
    flags, command, dbname, read_preference, slave_ok, check_keys, opts, ctx=None
):
    """Temporary hack to overwrite the _op_msg function in pymongo to work around a current bug with ProvenDB. This has been reported to the developers and should be fixed soon."""
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

    def __getitem__(self, name: Any) -> Collection:
        return self.db[name]

    def bulk_load_start(self) -> BulkLoadStartResponse:
        """Starts a bulk load on the database. Bulk loads allow multiple inserts without incrementing the version.
        See https://provendb.readme.io/docs/bulkload

        :raises BulkLoadException: A PyProven exception raised when db fails to start bulk loading, usually because the db is already bulk loading.
        :return: A dict-like object that holds the current version.
        :rtype: BulkLoadStartResponse
        """
        try:
            response = self.db.command("bulkLoad", BulkLoadEnums.START.value)
            return BulkLoadStartResponse(response)
        except PyMongoError as err:
            if (
                extract_error_info(err)["errmsg"]
                == "unable to start bulk load when bulk load already in progress"
            ):
                raise BulkLoadAlreadyStartedError(err) from None
            else:
                raise

    def bulk_load_stop(self) -> BulkLoadStopResponse:
        """Stops a bulk load on a database, failing if there is any outstanding operations.
        See https://provendb.readme.io/docs/bulkload

        :raises BulkLoadNotStartedException: Error when trying to stop a bulk load when the database is not bulk loading.
        :raises BulkLoadException: A PyProven exception when failing to stop a bulk load.
        :return: A dict-like object representing the response from the database.
        :rtype: BulkLoadStopResponse
        """
        response = self.db.command("bulkLoad", BulkLoadEnums.STOP.value)
        return BulkLoadStopResponse(response)

    def bulk_load_kill(self) -> BulkLoadKillResponse:
        """Stops a bulk load on a database, killing any remaining operations.
        See https://provendb.readme.io/docs/bulkload

        :raises BulkLoadNotStartedException: Error when attempting to kill a bulk load when one hasn't started.
        :raises BulkLoadException: Default exception when database fails to kill a bulkload.
        :return: A dict-like object containing the response from the database.
        :rtype: BulkLoadKillResponse
        """
        response = self.db.command("bulkLoad", BulkLoadEnums.KILL.value)
        return BulkLoadKillResponse(response)

    def bulk_load_status(self) -> BulkLoadStatusResponse:
        """Returns the current bulk load status of the database.
        See https://provendb.readme.io/docs/bulkload

        :raises BulkLoadException: Generic error when database fails to check bulk load status.
        :return: A dict-like object holding the current bulk load status of the database.
        :rtype: BulkLoadStatusResponse
        """
        response = self.db.command("bulkLoad", BulkLoadEnums.STATUS.value)
        return BulkLoadStatusResponse(response)

    def compact_versions(
        self,
        start_version: int,
        end_version: int,
        destroy_proofs: Optional[bool] = None,
    ) -> CompactResponse:
        """Compacts all proofs, versions and documents in the db between two given versions,
        deleting all data that only exists between the two versions.
        See https://provendb.readme.io/docs/compact

        :param start_version: The first version to compact from.
        :type start_version: int
        :param end_version: The last version to compact to.
        :type end_version: int
        :raises CompactException: pyproven exception when ProvenDB fails to compact between the two given versions.
        :return: A dict-like object containing the number of deleted proofs, versions and documents.
        :rtype: CompactResponse
        """
        command_args = SON({"startVersion": start_version, "endVersion": end_version})
        if destroy_proofs:
            command_args.update({"destroyProofs": destroy_proofs})
        try:
            response = self.db.command("compact", command_args)
            return CompactResponse(response)
        except PyMongoError as err:
            error_msg = extract_error_info(err)["errmsg"]
            if (
                error_msg
                == "There must be a full proof above the range to be compacted"
            ):
                raise CompactProofError(err)
            else:
                raise

    def create_ignored(self, collection: str) -> CreateIgnoredResponse:
        """Sets a collection to be ignored; it will  be identical among versions, not include metadata,
        and not included in proofs.
        See https://provendb.readme.io/docs/ignored-collections

        :param collection: The name of the collection to be ignored.
        :type collection: str
        :return: A dict-like object with the time of the operation.
        :raises CreateIgnoredException: pyproven exception when database fails to ignore the given collection.
        :rtype: CreateIgnoredResponse
        """
        response = self.db.command("createIgnored", collection)
        return CreateIgnoredResponse(response)

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
        command_args = SON({"collection": collection, "filter": filter})
        if projection:
            command_args.update({"projection": projection})
        response = self.db.command("docHistory", command_args)
        return DocumentHistoryResponse(response)

    def forget_prepare(
        self,
        collection: str,
        filter: Dict[str, Any],
        min_version: Optional[int] = None,
        max_version: Optional[int] = None,
        inclusive_range: Optional[bool] = None,
    ) -> PrepareForgetResponse:
        """Prepares an operation to forget a set of documents. This will erase the data but preserve hashes so as to verify proofs.
        See https://provendb.readme.io/docs/forget

        :param collection: The name of the collection to forget documents from.
        :type collection: str
        :param filter: A filter that choses the specific documents to forget.
        :type filter: Dict[str, Any]
        :param min_version: Minimum version to forget documents in. Defaults to first version of database.
        :type min_version: Optional[int], optional
        :param max_version: Maximum verstion to forget documents to. Defaults to current version of database.
        :type max_version: Optional[int], optional
        :param inclusive_range: If true, forget documents that ONLY exist between the two versions, defaults to True
        :type inclusive_range: bool, optional
        :raises PrepareForgetException: Generic error when failing to prepare forget operation on database.
        :return: A dict-like object that holds the forget password as well as forget summary.
        :rtype: PrepareForgetResponse
        """
        command_args = SON(
            {
                "collection": collection,
                "filter": filter,
            }
        )
        if min_version:
            command_args.update({"minVersion": min_version})
        if max_version:
            command_args.update({"maxVersion": max_version})
        if inclusive_range:
            command_args.update({"inclusiveRange": inclusive_range})
        response = self.db.command("forget", {"prepare": command_args})
        return PrepareForgetResponse(response)

    def forget_execute(self, forget_id: int, password: str) -> ExecuteForgetResponse:
        """Executes a prepared forget operation, deleting data but preserving hashes.
        See https://provendb.readme.io/docs/forget

        :param forget_id: Id given in prepare operation.
        :type forget_id: int
        :param password: Password given in prepare operation.
        :type password: str
        :raises PrepareForgetException: Generic exception when database fails to execute a forget operation.
        :return: A dict-like object returning the status and summary of the forget operation.
        :rtype: ExecuteForgetResponse
        """
        command_args = SON({"forgetId": forget_id, "password": password})
        response = self.db.command("forget", {"execute": command_args})
        return ExecuteForgetResponse(response)

    def get_document_proof(
        self,
        collection: str,
        filter: Dict[str, Any],
        version: int,
        proof_format: Optional[str] = None,
    ) -> GetDocumentProofResponse:
        """Filters documents in a collection and returns any proofs of those documents for a given version.
        See: https://provendb.readme.io/docs/getdocumentproof
        :param collection: The name of the collection to filter.
        :type collection: str
        :param filter: A mongodb filter that subsets the collection.
        :type filter: Dict[str, Any]
        :param version: The version number to fetch proofs for.
        :type version: int
        :param proof_format: The format of the proof, either 'binary' or 'json', defaults to "json"
        :type proof_format: str
        :raises GetDocumentProofException: [description]
        :return: A dict-like object containing an array of document proof documents.
        :rtype: GetDocumentProofResponse
        """
        command_args = SON(
            {
                "collection": collection,
                "filter": filter,
                "version": version,
            }
        )
        if proof_format:
            command_args.update({"proofFormat": proof_format})
        response = self.db.command("getDocumentProof", command_args)
        return GetDocumentProofResponse(response)

    def get_version(self) -> GetVersionResponse:
        """Gets the version the db is set to.
        See https://provendb.readme.io/docs/getversion

        :return: A dict-like object representing the ProvenDB return document.
        :raises GetVersionException: pyproven exception when db fails to return the current version.
        :rtype: GetVersionData
        """
        response = self.db.command("getVersion", 1)
        return GetVersionResponse(response)

    def get_version_proof(
        self,
        proof_id: Union[str, int],
        proof_format: Optional[str] = None,
        list_collections: Optional[bool] = None,
    ) -> GetVersionProofResponse:
        """Gets a proof for a specific database version.
        See https://provendb.readme.io/docs/getproof

        :param proof_id: Either a string matching a proofId, or a version number.
        :type proof_id: Union[str, int]
        :param proof_format: Format type of proof, either 'binary' or 'json', defaults to json
        :type proof_format: str, optional
        :param list_collections: If True all collections in proof are listed, defaults to False.
        :type list_collections: bool, optional
        :raises GetVersionProofException:
        :return: A dict-like object holding an array of proofs.
        :rtype: GetVersionProofResponse
        """
        command_args: SON = SON({"getProof": proof_id})
        if proof_format:
            command_args.update({"format": proof_format})
        if list_collections:
            command_args.update({"listCollections": list_collections})
        response = self.db.command(command_args)
        return GetVersionProofResponse(response)

    def list_storage(self) -> ListStorageResponse:
        """Fetches the storage size for each collection in the db.
        See https://provendb.readme.io/docs/liststorage

        :return: A dict-like object holding a list of dict-like objects,
        each containg a single 'collection_name: collection_storage_size' key-value pair.
        :rtype: ListStorageResponse
        """
        response = self.db.command("listStorage")
        return ListStorageResponse(response)

    def list_versions(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
        sort_direction: Optional[int] = None,
    ) -> ListVersionsResponse:
        """Retrieves a list of versions given a search parameter.
        See https://provendb.readme.io/docs/listversions

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
        command_args = SON()
        if start_date:
            command_args.update({"startDate": start_date})
        if end_date:
            command_args.update({"endDate": end_date})
        if limit:
            command_args.update({"limit": limit})
        if sort_direction:
            command_args.update({"sortDirection": sort_direction})
        response = self.db.command({"listVersions": command_args})
        return ListVersionsResponse(response)

    def rollback(self) -> RollbackResponse:
        """Rolls back the database to the last valid version, cancelling any current insert, update or delete operations.
        See https://provendb.readme.io/docs/rollback

        :return: A dict-like object holding the 'db_name: db_version' pair the db has been rolled back to.
        :rtype: RollbackResponse
        """
        response = self.db.command("rollback")
        return RollbackResponse(response)

    def set_version(
        self, date: Union[str, int, datetime.datetime]
    ) -> SetVersionResponse:
        """Sets the database version to a given version identifier.
        See https://provendb.readme.io/docs/setversion

        :param date: Version number, string literal 'current', or :class:`datetime.datetime` object.
        :type date: Union[str,int,datetime]
        :raises SetVersionException: pyproven exception when db fails to set the given version.
        :return: A dict-like object representing the provenDB return document.
        :rtype: SetVersionData
        """
        response = self.db.command("setVersion", date)
        return SetVersionResponse(response)

    def show_metadata(self) -> ShowMetadataResponse:
        """Causes the db to also show ProvenDB metadata on documents.
        See https://provendb.readme.io/docs/showmetadata

        :return: A dict-like object holding the 'ok' response from the database.
        :rtype: ShowMetadataResponse
        """
        response = self.db.command("showMetadata", True)
        return ShowMetadataResponse(response)

    def hide_metadata(self) -> HideMetadataResponse:
        """Causes the db to hide ProvenDB metadata on documents.
        See https://provendb.readme.io/docs/showmetadata

        :return: A dict-like object holding the 'ok' response from the database.
        :rtype: HideMetadataResponse
        """
        response = self.db.command("showMetadata", False)
        return HideMetadataResponse(response)

    def submit_proof(
        self,
        version: int,
        collections: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        anchor_type: Optional[str] = None,
        n_checks: Optional[int] = None,
    ) -> SubmitProofResponse:
        """Creates a proof for a version and inserts it on the blockchain.
        See https://provendb.readme.io/docs/submitproof

        :param version: The version number to be proved.
        :type version: int
        :param collections: The collections to be included, defaults to all.
        :type collections: Optional[List[str]],
        :param filter: A MongoDB filter that selects documents within the collections, defaults to all documents.
        :type filter: Optional[Dict[str, Any]], optional
        :param anchor_type: The blockchain used to anchor the proof, defaults to ETH_MAINNET.
        :type anchor_type: Optional[str], optional
        :param n_checks: Number of times the proof hash will be recalculated
        :type n_checks: Optional[int], optional
        :return: A dict-like object holding the proof data.
        :rtype: SubmitProofResponse
        """
        command_args: SON = SON({"submitProof": version})
        if collections:
            command_args.update({"collections": collections})
        if filter:
            command_args.update({"filter": filter})
        if anchor_type:
            command_args.update({"anchorType": anchor_type})
        if n_checks:
            command_args.update({"nChecks": n_checks})
        response = self.db.command(command_args)
        return SubmitProofResponse(response)

    def verify_proof(
        self, proof_id: str, format: Optional[str] = None
    ) -> VerifyProofResponse:
        """Verifies a proof previously uploaded to the blockchain.
        See https://provendb.readme.io/docs/verifyproof

        :param proof_id: The id of the proof to validate.
        :type proof_id: str
        :param format: Format of the proof document, defaults to json
        :type format: Optional[str]
        :return: A dict-like object holding the proof and proof information.
        :rtype: VerifyProofResponse
        """
        command_args: SON = SON({"verifyProof": proof_id})
        if format:
            command_args.update({"format": format})
        response = self.db.command(command_args)
        return VerifyProofResponse(response)
