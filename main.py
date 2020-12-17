if __name__ == "__main__":    
    import os
    from pymongo import MongoClient
    mongoURI = os.getenv("PROVENDB_URI")
    print(f'connecting w/ URI {mongoURI}')
    mclient=MongoClient(mongoURI)
    db=mclient["python-test"]
    command = {'getVersion':1}
    getVersionOutput=db.command(command)
    print('The version is %s' % (getVersionOutput.version))