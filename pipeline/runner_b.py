from itertools import islice
from pipeline.orch import getdata
from pipeline.faceitclient import FaceitClient

import time
import datetime
from zoneinfo import ZoneInfo

from configs.exceptions import BatchError
from configs.exceptions import NoCheckpoint

import sys
import logging
import json
from pathlib import Path
import os
import asyncio
import aiohttp

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
        self.batch_size = 30
        self.max_players = len(list(self.data.players.find({'_id': {"$exists": True}})))

        self.batch_flag = None

        # Checkpointing Variables
        self.batch_count = 0
        self.last_saved_checkpoint_idx = self.most_recent_checkpoint()
        self.retrieved_player_id = 0
        self.lifetime_map_scores = 0

        # benchmark api latency tracking variables
        self.now = None
        self.then = None
        self.execution_time = 0
        
        # Request/Response logs
        self.runner_response_details = {}

        self.data_references = {"alters":self.data.alters_batch,
                "matches":self.data.matches_batch,
                "ratings":self.data.ratings_batch,
                "players":self.data.players_batch,
                #lifetime":self.lifetime_map_scores,
                "matches_elo":self.data.matches_elo_batch}
    
    async def supermatch(self):
        """
        Supermatch default state is to pick from a save, if save not found it starts fetching from scratch.
        else, start from scratch if default_state = False

        :helper_supermatch(): helper supermatch runs the statistics, lifetime and matches_elo endpoint calls.
        """
        # async client session
        async with aiohttp.ClientSession() as session:

            start = time.perf_counter()

            self.retrieved_player_id = self.most_recent_checkpoint()
            if self.retrieved_player_id == 0:
                default_state = False
                self.logger.warning("No checkpoint was found!")
            else: 
                default_state = True
            try:  
                # Collect N player_ids
                indices_array, player_ids = self.client.collect_N()
                # Get statistics of all the existing matches
                players_stats = []
                matches_elo_stats = []

                if default_state:
                    index = player_ids.index(self.retrieved_player_id)
                    self.logger.info(len(player_ids))
                    shortened_player_ids = player_ids[index-1: ]
                    shortened_indices_array = indices_array[1: index]

                    # This variable allows for tracking of players from checkpoint
                    indices_array_for_count = indices_array[index-1: ]

                    await self.helper_supermatch(indices_array= shortened_indices_array, 
                                player_ids= shortened_player_ids, 
                                count = indices_array_for_count,
                                stats= (players_stats, matches_elo_stats),
                                session = session,
                                default_state = default_state,
                                start = start
                                )
                else:
                    indices_array_for_count = indices_array 
                        
                    await self.helper_supermatch(indices_array= indices_array, 
                                    player_ids= player_ids,
                                    count = indices_array_for_count,
                                    stats=(players_stats, matches_elo_stats),
                                    session = session,
                                    default_state = default_state,
                                    start = start)  
                                
                #end = time.perf_counter()
                #print(f"Execution time : {end - start:.6f} seconds")
                        
            except Exception as e:
                self.logger.error(f"Error occured in supermatch: {e}", exc_info= True)
                self.checkpointer()

            except BaseException:
                self.checkpointer()
                raise BatchError(f"Batch failed processing at batch count: {self.batch_count}")
    

    async def helper_supermatch(self, indices_array, player_ids, count, stats, session, default_state, start):
        # accessing player ids for alter func

        # Async concurrent workers
        concurrent_workers = 10
        semaphore = asyncio.Semaphore(concurrent_workers)

        async def process_player(idx):
            async with semaphore:

                # 'success' acts as a flag and also contains alter_match_ids,
                # to be used by statistics functionlast
                self.logger.info(
                    f"({count[idx]}) processing player id :{player_ids[idx]}"
                )  

                alter_match_ids = await self.client.retry_function(
                    self.client.alter_function, player_ids[idx], session
                ) or []
                
                alter_match_ids =  list(set(alter_match_ids))
                n = len(alter_match_ids)

                # THIS IS SKIPPING THE PLAYER DUE TO LESS MATCHES
                if not alter_match_ids:
                    self.logger.warning(
                        f"({count[idx]}) Player ID skipped due lesser number of matches in the past 40d!!"
                    )
                    return None

                # Processing each match id from the randomizer
                stats_tasks = [
                    self.client.retry_function(
                        self.client.statistics_transform, match_id, session, count[idx]
                    )
                    for match_id in alter_match_ids
                ]

                elo_tasks = [
                    self.client.retry_function(
                        self.client.matches_elo, match_id, session, count[idx]
                    )
                    for match_id in alter_match_ids
                ]

                results = await asyncio.gather(
                    *stats_tasks,
                    *elo_tasks,
                    self.client.lifetime_aggregates(player_ids[idx], session),
                    return_exceptions=True
                )

                stats_results = results[:n]
                elo_results = results[n:2*n]
                lifetime_result = results[-1]

                value_stats = [r for r in stats_results if r and not isinstance(r, Exception)]
                value_elo = [r for r in elo_results if r and not isinstance(r, Exception)]

                if default_state:
                    self.logger.info(
                        f"({count[idx]}) Ran player id :{player_ids[idx]}"
                    )  
                else:
                    self.logger.info(
                        f"({count[idx]+1}) Ran player id :{player_ids[idx]}"
                    )  
                
                return {
                    "idx": idx,
                    "stats": value_stats,
                    "elo": value_elo,
                    "lifetime": lifetime_result
                }

        # player level asynchronous workers
        #tasks = []
        #for idx in indices_array:
        #    tasks.extend(process_player(idx))
            # if tasks hits a len of 15, batch unlock --> process it once (one coroutine)
        print(len(indices_array))
        tasks = [process_player(idx) for idx in indices_array]
        batch_count = 0
    
        for chunks in self.chunked(tasks, 15):
            
            results = await asyncio.gather(*chunks, return_exceptions=True)
            
            for res in results:
                if not res:
                    continue
                if isinstance(res,Exception):
                    self.logger.error(f"Chunk error {res}")

                idx = res['idx']

                stats_temp = res['stats']
                self.data.ratings_batch.extend(stats_temp)

                elo_temp = res['elo']
                self.data.matches_elo_batch.extend(elo_temp)

                #lifetime api call
                lifetime_temp = (
                    res["lifetime"] if not isinstance(res["lifetime"], Exception) else None
                )
                if lifetime_temp is not None:
                    self.data.store_data(batch=lifetime_temp, collection="lifetime", verbose=True)
                    self.logger.info("Stored Lifetime: %s", lifetime_temp['_id'])
                #self.batch_processor(lifetime_scores= lifetime_temp)

                # Saving last checkpoint for player id
                self.last_saved_checkpoint_idx = player_ids[idx]

            # running batch processor and clearing state variables from orch.py
            self.batch_processor(batchId = batch_count)

            for batches in self.data_references.values():
                batches.clear()

            batch_count += 1
        #results = await asyncio.gather(*tasks)

        # collecting results
        # for res in results:
        #     if not res:
        #         continue

        #     idx = res["idx"]

        #     # clearing player_stats prevents redundancy
        #     #stats[0].extend(res["stats"])
        #     stats_temp = res['stats']
        #     self.data.ratings_batch.extend(stats_temp)
        #     #stats[0].clear()

        #     # clearing matches_elo prevents redundancy
        #     #stats[1].extend(res["elo"])
        #     elo_temp = res['elo']
        #     self.data.matches_elo_batch.extend(elo_temp)

        #     #stats[1].clear()

        #     # lifetime api call
        #     self.lifetime_map_scores = (
        #         res["lifetime"] if not isinstance(res["lifetime"], Exception) else None
        #     )

        #     # Saving last checkpoint for player id
        #     self.last_saved_checkpoint_idx = player_ids[idx]

        #     if default_state:
        #         await self.batch_processor()
        #         self.logger.info(
        #             f"({count[idx]}) Ran player id :{player_ids[idx]}, alters and matches stored!"
        #         )  
        #     else:
        #         await self.batch_processor()
        #         self.logger.info(
        #             f"({count[idx]+1}) Ran player id :{player_ids[idx]}, alters and matches stored!"
        #         ) 

        end = time.perf_counter()
        self.execution_time = end - start
        self.logger.info(f"Execution time : {self.execution_time:.4f} seconds")
    
    
    def chunked(self, iterable, batch_size):

            iterables = iter(iterable)
            while True:
                batch = list(islice(iterables, batch_size))

                if not batch:
                    break
                else:
                    yield batch

    def batch_processor(self, lifetime_scores = None, batchId = None):
                
        if lifetime_scores is None:

            for collection, batches in self.data_references.items():
                current_batches = list(batches)

                # Guard
                if not current_batches:
                    continue
                
                if len(current_batches) >= self.batch_size or self.client.error_flag: # add >= not ==
                    # batch flag allows to iterate batch count
                    #self.batch_flag = True
                    
                    # chunking batches
                    for batch in self.chunked(current_batches, self.batch_size):

                        # adding batchId to each batch
                        for item in batch:
                            item['batchId'] = batchId

                        # storing data into DB 
                        self.data.store_data(batch = batch,
                                            collection = collection,
                                            verbose = True)
                    #batches.clear()
            self.logger.info(
                f"IDs -> {batchId*15}-{(batchId*15)+15}, data stored!"
            ) 
            #if self.batch_flag:
            #    self.batch_count +=1
        # else:
        #     if lifetime_scores:
        #         self.data.store_data(
        #             batch = lifetime_scores,
        #             collection = 'lifetime',
        #             verbose = True
        #         )

        # if self.lifetime_map_scores:
        #     await self.data.store_data(
        #         batch=self.lifetime_map_scores,
        #         collection="lifetime",
        #         verbose=True
        #     )

        #self.batch_flag = False

        # for every single batch sent, two fields always go with it batchId, stageId
            

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
        
        if self.client.sum_of_latency == 0 or self.client.sum_of_requests == 0:
            rpm = 0
            avg_latency = 0
        else:
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

            if not files:
                self.logger.warning("No checkpoint found.")
                return 0


            timestamps = {idx: {"name":file,
                                "time":(time.time() - os.path.getctime(file))}
                        for idx, file in enumerate(files)}
            
            min_value = min([time.get("time") for time in timestamps.values()])
            retrieved_path = str([path 
                                for path in timestamps.values() 
                                if path.get("time") == min_value][0]['name'])
            
            with open(retrieved_path, 'r') as f:
                save = json.load(f)

            if not save['player_id']:
                self.logger.warning("No checkpoint found.")
                save['player_id'] = 0

                return save['player_id']
            
            if save['player_id'] != 0:
                self.logger.info("Retrieved most recent checkpoint : %s", save['player_id'])
                return save['player_id']
            
            else:
                
                raise ValueError

        except (ValueError, KeyError) as e:
            raise NoCheckpoint("No Checkpoint save was found!")
        
       
    

            
            


        
        
        





