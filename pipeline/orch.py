from pymongo import MongoClient, errors
import pandas as pd
import logging

class getdata():
    def __init__(self):
        # Used for terminating the DataExporter loop --> runner.py
        self.batch_flag = True
        self.logger = logging.getLogger(__name__)

    def connect_db(self, host, port, connect = True):

        self.client = MongoClient(
            host = host,
            port = port,
            connect = connect
        )

        try:
            self.client.admin.command('ping')

            self.logger.info("Connected to MongoDB")
            self.logger.info(f"List of Databases :{self.client.list_database_names()}")

        except:
            self.logger.critical("No database found on host, check host & port")
        
        # Database Objects
        self.db = self.client['FaceitDB']
        self.matches = self.db['matches']
        self.players = self.db['players']
        self.ratings = self.db['ratings']
        self.alters = self.db['alters']
        
        # Batches ready to be stored
        self.matches_batch = []
        self.players_batch = []
        self.ratings_batch = []
        self.alters_batch = []


    def store_data(self, batch, collection:str, verbose = False):

        """
        Docstring for store_data
        
        param input: Batch to store in db
        param collection: collection object name
        param verbose: Allows for preview of data incoming, can make script slow
        """

        if verbose == True:
            self.logger.debug(f"Preview of data incoming: \n {pd.DataFrame(batch).head()}")
        
        #if batch != list:
        #    batch = list(batch)
        try:
            if collection not in ['matches', 'players', 'ratings', 'alters']:
                self.logger.error('Enter the correct collection name')
            else:
                if collection.lower() == "matches":
                    self.stored = self.matches.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    self.logger.info("Data moved sucessfully. \n Database :%s \n Collection:%s", self.db, collection)
                if collection.lower() == "players":
                    self.stored = self.players.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    self.logger.info("Data moved sucessfully. \n Database :%s \n Collection:%s", self.db, collection)
                if collection.lower() == "ratings":
                    self.stored = self.ratings.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

                    self.logger.info("Data moved sucessfully. \n Database :%s \n Collection:%s", self.db, collection)
                if collection.lower() == "alters":
                    self.stored = self.alters.insert_many(
                    documents = batch,
                    # skipping duplicates
                    ordered = False)

        # Note: PyMongoError class exception isnt written

        except errors.BulkWriteError as e:
            self.logger.error("\nTable: %s", collection)
            errmsg = e.details['writeErrors'][0]['errmsg']
            self.logger.debug("Error: %s\n", errmsg)



    def getcol(self, db, col):
        """
        Function outputs collection name
        """
        return self.client[db][col]

    def querydb(self):
        
        pass

        


        


