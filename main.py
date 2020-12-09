import os
import pymongo
from src.pyproven.database.database import ProvenDB
from typing import List,Dict
if __name__ == "__main__":    
    if (URI := os.getenv("PROVENDB_URI")) is None:
        input("Enter ProvenDB URI")
    if (DATABASE := os.getenv("PROVENDB_DATABASE")) is None:
        input("Enter ProvenDB database name")
    client: pymongo.MongoClient = pymongo.MongoClient(URI)
    db: pymongo.database.Database = client[DATABASE]
    pdb = ProvenDB(db)