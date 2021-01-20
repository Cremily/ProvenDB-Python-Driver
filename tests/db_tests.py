import datetime
from pyproven.storage import ListStorageDocuments, ListStorageResponse
from typing import List

import unittest

import os

from pymongo import MongoClient

from pyproven.exceptions import BulkLoadException, SetVersionException
from pyproven import ProvenDB

PROVENDB_URI = os.getenv("PROVENDB_URI")
PROVENDB_DATABASE = os.getenv("PROVENDB_DB")


class ProvenDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MongoClient(PROVENDB_URI)
        self.db = self.client[PROVENDB_DATABASE]
        self.pdb = ProvenDB(self.db, provendb_hack=True)

    def test_proven_constructor(self):
        """PyProven can create a ProvenDB object."""
        self.assertIsInstance(self.pdb, ProvenDB)

    def test_bulk_load(self):
        """PyProven can start and then stop a bulkload operation, and check the current bulkload status."""
        try:
            self.pdb.bulk_load_start()
            status = self.pdb.bulk_load_status()
            self.assertTrue(status.status == "on")
            self.pdb.bulk_load_stop()
            status = self.pdb.bulk_load_status()
            self.assertTrue(status.status == "off")
        except Exception as err:
            try:
                self.pdb.bulk_load_kill()
            except BulkLoadException:
                pass
            raise err

    def test_get_version(self):
        """PyProven can get the version the DB is set to."""
        version = self.pdb.get_version()
        self.assertTrue("The version is set to: " in version.response)

    def test_set_version_first(self):
        """PyProven can set version to the first version of the DB."""
        version = self.pdb.set_version(1)
        self.assertTrue(version.version == 1)

    def test_set_version_current(self):
        """PyProven can set version to the most current DB version."""
        version = self.pdb.set_version("current")
        self.assertTrue(version.response == "The version has been set to: 'current'")

    def test_set_version_impossible(self):
        """PyProven will raise the correct exception when given an impossible version number."""
        version = self.pdb.set_version("current")
        impossible_version = version.version + 1000
        with self.assertRaises(SetVersionException):
            self.pdb.set_version(impossible_version)

    def test_list_versions_noargs(self):
        """PyProven can call list_versions with no arguments and always presents at least the current version."""
        versions = self.pdb.list_versions()
        self.assertTrue(versions)

    def test_list_versions_limit(self):
        """PyProven correctly limits the number of returned versions in a list_versions command."""
        versions = self.pdb.list_versions(limit=1)
        self.assertTrue(len(versions["versions"]) == 1)

    def test_doc_history(self):
        """Pyproven can correctly get the history of documents in a filtered collection"""
        history = self.pdb.doc_history("unit-test", {"a": 1})
        self.assertTrue(history.history)

    def test_list_storage(self):
        """Pyproven can correctly list the storage of all collections in the database."""

        def _collection_in_storage_doc(
            col_name: str, storage_list: ListStorageResponse
        ):
            for storage_doc in storage_list:
                if col_name in storage_doc.keys():
                    return True
            return False

        storage_list = self.pdb.list_storage().storageList
        collection_list = [
            name for name in self.db.list_collection_names() if name[0] != "_"
        ]

        for col_name in collection_list:
            self.assertTrue(_collection_in_storage_doc(col_name, storage_list))

    def test_metadata_shows(self):
        """PyProven can show metadata and then hide metadata."""
        self.pdb.show_metadata()
        self.assertTrue("_provendb_metadata" in self.pdb["unit-test"].find_one())
        self.pdb.hide_metadata()
        self.assertTrue("provendb_metadata" not in self.pdb["unit-test"].find_one())


if __name__ == "__main__":
    unittest.main()
