from itertools import islice
from pipeline.orch import getdata

import time
import datetime
from zoneinfo import ZoneInfo

from pipeline.faceitclient import FaceitClient
from configs.exceptions import BatchError
from configs.exceptions import NoCheckpoint

import sys
import logging
import json
from pathlib import Path
import os

from time import perf_counter
from configs.exceptions import SkippingMatch
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
        self.batch_size = 50
        self.max_players = len(list(self.data.players.find({'_id': {"$exists": True}})))

        self.batch_flag = None

        # Checkpointing Variables
        self.batch_count = 0
        self.last_saved_checkpoint_idx = 0
        self.retrieved_player_id = 0
        self.lifetime_map_scores = 0

        # benchmark api latency tracking variables
        self.now = None
        self.then = None
        self.execution_time = 0
        
        # Request/Response logs
        self.runner_response_details = {}

    
    def supermatch(self):
        """
        Supermatch default state is to pick from a save, if save not found it starts fetching from scratch.
        else, start from scratch if default_state = False
        """

        start = time.perf_counter()
        default_state = True

        def helper_supermatch(indices_array, player_ids, stats):
            # accessing player ids for alter func
            for idx in indices_array:

                # 'success' acts as a flag and also contains alter_match_ids,
                #  to be used by statistics functionlast
                alter_match_ids = self.client.retry_function(self.client.alter_function, 
                                                    player_ids[idx])
                
                # THIS IS SKIPPING THE PLAYER DUE TO LESS MATCHES
                if not alter_match_ids:
                    self.logger.warning(f"({idx+1}) Player ID skipped due lesser number of matches in the past 40d!!")
                    continue

                self.lifetime_map_scores = self.client.retry_function(self.client.lifetime_aggregates, player_ids[idx])                
                self.logger.info(f"Lifetime Aggregates captured! player : {player_ids[idx]}")

                for match_id in alter_match_ids:
                    #try:
                
                    statistics = self.client.retry_function(
                        self.client.statistics_transform, match_id
                    )
                    
                    # matches elo api end point persistence
                    if match_id not in self.client.elo_persistence:
                        matches_elo_stats = self.client.retry_function(
                            self.client.matches_elo, match_id
                        )
                    
                    else:
                        self.logger.info("match's elo is persistence, not calling API.")
                        continue
                    
                    if match_id is not None:
                        self.client.elo_persistence.add(match_id)

                    # THIS IS SKIPPING THE MATCH DUE TO NO RESPONSE
                    if statistics is not None:
                        stats[0].append(statistics)

                    elif statistics is None:
                        continue
                    
                    if matches_elo_stats is not None:
                        stats[1].append(matches_elo_stats)
                    
                    # THIS IS SKIPPING THE MATCH DUE TO NO RESPONSE
                    elif matches_elo_stats is None:
                        continue
                    
                    #except SkippingMatch as e:
                    #    self.logger.warning(f"Skipping match at runner level: {match_id}")
                    #    continue

                
                

                # clearing player_stats prevents redundancy
                self.data.ratings_batch.extend(stats[0])
                stats[0].clear()

                # clearing matches_elo prevents redundancy
                self.data.matches_elo_batch.extend(stats[1])
                stats[1].clear()

                # Saving last checkpoint for player id
                self.last_saved_checkpoint_idx = player_ids[idx]

                # retrieving lifetime aggregate statistics of player_ids[idx]
                #self.data.lifetime_batch.extend(stats[1])
            
                if default_state:
                    self.batch_processor()
                    self.logger.info(f"({idx+1}) Ran player id :{player_ids[idx]}, alters and matches stored!")  

                else:
                    self.batch_processor()
                    self.logger.info(f"({idx+2}) Ran player id :{player_ids[idx]}, alters and matches stored!") 
            

                end = time.perf_counter()
                self.execution_time = end - start
                self.logger.info(f"Execution time : {self.execution_time:.4f} seconds")
        try:  
            # Collect N player_ids
            indices_array, player_ids = self.client.retry_function(self.client.collect_N)

            # Get statistics of all the existing matches
            players_stats = []
            matches_elo_stats = []

            if default_state:
                try:
                    self.retrieved_player_id = self.most_recent_checkpoint()
                    index = player_ids.index(self.retrieved_player_id)

                    player_ids = player_ids[index-1: ]
                    indices_array = indices_array[index-1: ]

                    helper_supermatch(indices_array= indices_array, 
                              player_ids= player_ids, 
                              stats= (players_stats, matches_elo_stats))
                    

                except NoCheckpoint:
                    pass

            helper_supermatch(indices_array= indices_array, 
                              player_ids= player_ids, 
                              stats=(players_stats, matches_elo_stats))
            #end = time.perf_counter()
            #print(f"Execution time : {end - start:.6f} seconds")
                    
        except Exception as e:
            self.logger.error(f"Error occured in supermatch: {e}", exc_info= True)
            self.checkpointer()

        except BaseException:
            self.checkpointer()
            raise BatchError(f"Batch failed processing at batch count: {self.batch_count}")
        
    
    
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
                "players":self.data.players_batch,
                "lifetime":self.lifetime_map_scores,
                "matches_elo":self.data.matches_elo_batch}
        
        for collection, batches in data.items():
            #batch_flag = False

            if len(batches) >= self.batch_size or self.client.error_flag: # add >= not ==
                #batch_flag = True
                #self.batch_count += 1

                for batch in self.chunked(batches, self.batch_size):
                    self.data.store_data(batch = batch,
                                        collection = collection,
                                        verbose = True)
                batches.clear()

        if self.lifetime_map_scores:
            self.data.store_data(
                batch=self.lifetime_map_scores,
                collection="lifetime",
                verbose=True
            )

                

        self.batch_count += 1

    def checkpointer(self):
        """
        
        checkpointer a wrapper for batch processing,
        and saves the state of a failed batch.
        """
        saves = {}
    
        self.logger.warning('checkpoint triggered due to sudden shutdown')
        self.logger.warning('saving recent checkpoint:\n @player_id %s \n @batch count %s', self.last_saved_checkpoint_idx, self.batch_count)
        
        zt_uae = ZoneInfo("Asia/Dubai")

        current_time = time.time()
        current_time_utc = datetime.datetime.fromtimestamp(current_time, datetime.timezone.utc)
        current_time_uae =  current_time_utc.astimezone(zt_uae)
        date = current_time_uae.strftime("%d %b %Y %I %M%p").replace(" ", "_")
        

        saves = {"time": str(current_time_uae),
                "player_id": self.last_saved_checkpoint_idx}
        
                #"batch_count": self.batch_count}
        
        # request/response details
        self.runner_response_details = self.client.response_details

        rpm = (self.client.sum_of_requests/self.client.sum_of_latency) * 60
        avg_latency = self.client.sum_of_latency/self.client.sum_of_requests

        self.runner_response_details['stats'] = {
            "RPM": rpm,
            "avg_latency": avg_latency,
            'retries_count': self.client.retry_count,
            "total_execution_time": self.execution_time + self.client.fail_overall_total
        }



        with open(f"checkpoints/checkpoint_saves_{date}.json", "w") as saves_json:
            json.dump(saves, saves_json, indent = 4)

        with open(f"response/response_details_{date}.json", "w") as response_json:
            json.dump(self.runner_response_details, response_json, indent = 4)
    
    def most_recent_checkpoint(self):
        """
        Extracts variables from the most recent checkpoint saved.
        
        """
        try:
            folder = "checkpoints"

            path = Path(folder)
            file_pattern = "*.json"
            files = list(path.glob(file_pattern))

            timestamps = {idx: {"name":file,
                                "time":(time.time() - os.path.getctime(file))}
                        for idx, file in enumerate(files)}
            
            min_value = min([time.get("time") for time in timestamps.values()])
            retrieved_path = str([path 
                                for path in timestamps.values() 
                                if path.get("time") == min_value][0]['name'])
            
            with open(retrieved_path, 'r') as f:
                save = json.load(f)

            if save['player_id'] != 0:
                return save['player_id']
            
            else:
                raise ValueError

        except (ValueError, KeyError) as e:
            raise NoCheckpoint("No Checkpoint save was found!")
        
       
    

            
            


        
        
        





