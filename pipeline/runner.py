from itertools import islice
from pipeline.orch import getdata
import time
from pipeline.faceitclient import FaceitClient
import sys
import logging

class PipelineRunner():
    """
    Docstring for PipelineRunner

    Stores the Runner and supermatch function, all together automates a strong workflow for
    raw data collection.
    
    - Load context (players, configs, batch size)
    - Orchestrate stages (not implement them)
    - Handle failures + checkpoints
    """

    def __init__(self, headers, host, port):

        self.logger = logging.getLogger(__name__)

        # Connecting MongoDB 
        self.data = getdata()
        self.data.connect_db(host = host, port = port)

        self.client = FaceitClient(dbobj= self.data,
                                   headers= headers)
        # Assuming 72 players x 15 matches = 1080 matches
        self.batch_size = 150
        self.max_players = len(list(self.data.players.find({'_id': {"$exists": True}})))
    
    
    def supermatch(self):

        try:  
            # Collect N player_ids
            indices_array, player_ids = self.client.retry_function(self.client.collect_N)

            # Get statistics of all the existing matches
            players_stats = []

            # accessing player ids for alter func
            for idx in indices_array:
                # 'success' acts as a flag and also contains alter_match_ids,
                #  to be used by statistics function
                alter_match_ids = self.client.retry_function(self.client.alter_function, 
                                                    player_ids[idx])
                
                if alter_match_ids:
                    success = True

                for match_id in alter_match_ids:
                    statistics = self.client.retry_function(
                        self.client.statistics_transform, match_id
                    )

                    if statistics is not None:
                        players_stats.append(statistics)
                
                # clearing player_stats prevents redundancy
                self.data.ratings_batch.extend(players_stats)
                players_stats.clear()

                if success:
                    self.batch_processor()
                    self.logger.info(f"({idx+1}) Ran player id :{player_ids[idx]}, alters and matches stored!")
                else: 
                    self.logger.warning(f"({idx+1}) Player ID skipped due lesser number of matches in the past 40d!!")

        except Exception as e:
            self.logger.error(f"Error occured in supermatch: {e}", exc_info= True)
            
    def chunked(self, iterable, batch_size):

            iterables = iter(iterable)
            while True:
                batch = list(islice(iterables, batch_size))

                if not batch:
                    break
                else:
                    yield batch

    def batch_processor(self):

        data = {"alters":self.data.alters_batch,
                "matches":self.data.matches_batch,
                "ratings":self.data.ratings_batch,
                "players":self.data.players_batch}
        
        for collection, batches in data.items():
            if len(batches) >= self.batch_size or self.client.error_flag: # add >= not ==
                for batch in self.chunked(batches, self.batch_size):
                    self.data.store_data(batch = batch,
                                        collection = collection,
                                        verbose = True)
                
                batches.clear()

    


