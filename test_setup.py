"""Used only to setup initial database for testing. This script only needs to be run once for each new ProvenDB database."""
import pymongo
from pyproven import ProvenDB
import os
if os.getenv("PROVENDB_URI"):
    PROVENDB_URI = os.getenv("PROVENDB_URI")
    PROVENDB_DATABASE = os.getenv("PROVENDB_DB")
else:
    #used for github actions that set user defined enviornment variables as such. 
    PROVENDB_URI = os.getenv("INPUT_PROVENDB_URI")
    PROVENDB_DATABASE = os.getenv("INPUT_PROVENDB_DB")


client = pymongo.MongoClient(PROVENDB_URI)
db = client[PROVENDB_DATABASE]
pdb = ProvenDB(db)
collection = pdb["unit-test"]
collection.insert_many([{"x":i} for i in range(100)])
