from pymongo import MongoClient
import pandas as pd

class getdata():
    def __init__(self):
        pass

    def connect_db(self, host, port, connect = True):

        self.client = MongoClient(
            host = host,
            port = port,
            connect = connect
        )

        try:
            self.client.admin.command('ping')

            print("Connected to MongoDB")
            print(f"List of Databases :{self.client.list_database_names()}")

        except:
            print("No database found on host, check host & port")
        
        # Database Objects
        self.db = self.client['FaceitDB']
        self.matches = self.db['matches']
        self.players = self.db['players']
        self.ratings = self.db['ratings']
        self.alters = self.db['alters']


    def store_data(self, batch, collection:str, verbose = False):

        """
        Docstring for store_data
        
        param input: Batch to store in db
        param collection: collection object name
        param verbose: Allows for preview of data incoming, can make script slow
        """

        if verbose == True:
            print(f"Preview of data incoming: \n {pd.DataFrame(batch).head()}")
            print(type(batch))
        
        #if batch != list:
        #    batch = list(batch)
        try:
            if collection not in ['matches', 'players', 'ratings', 'alters']:
                print('Enter the correct collection name')
            else:
                if collection.lower() == "matches":
                    self.stored = self.matches.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    print(f"Data moved sucessfully. \n Database:{self.db} \n Collection:{collection}")
                if collection.lower() == "players":
                    self.stored = self.players.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    print(f"Data moved sucessfully. \n Database:{self.db} \n Collection:{collection}")

                if collection.lower() == "ratings":
                    self.stored = self.ratings.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    print(f"Data moved sucessfully. \n Database:{self.db} \n Collection:{collection}")
                if collection.lower() == "alters":
                    self.stored = self.alters.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

        except Exception as e:
            print("Table: ", {collection})
            print(f"Error: {e}")

    def getcol(self, db, col):
        """
        Function outputs collection name
        """
        return self.client[db][col]

    def querydb(self):
        
        pass



        


