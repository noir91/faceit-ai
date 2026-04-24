from itertools import islice
from pipeline.orch import getdata
from pipeline.faceitclient import FaceitClient

import numpy as np
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
        self.batch_size = 2
        self.max_players = len(list(self.data.players.find({'_id': {"$exists": True}})))

        self.batch_flag = None

        # Queue for passing data from producer to consumer
        self.queue = asyncio.Queue(maxsize = 80)

        # Batch Variables
        self.batches = []
        self.batch_count = 0

        # Checkpointing Variables
        self.last_checkpoint_upstream = 0
        self.last_checkpoint_downstream = 0 #self.most_recent_checkpoint()
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
        else, start from scratch if start_from_checkpoint = False

        :helper_supermatch(): helper supermatch runs the statistics, lifetime and matches_elo endpoint calls.
        """
        # async client session
        async with aiohttp.ClientSession() as session:

            start = time.perf_counter()
            # Collect remaining players from Lookup 
            remaining_indices, remaining_pids = self.most_recent_checkpoint()
        
            if len(remaining_pids) == 0:
                start_from_checkpoint = False
                self.logger.warning("No checkpoint was found!")

            else: 
                start_from_checkpoint = True
            try:  
                # Get statistics of all the existing matches
                players_stats = []
                matches_elo_stats = []

                if start_from_checkpoint:
                    await self.helper_supermatch(indices_array= remaining_indices, 
                                player_ids= remaining_pids, 
                                count = remaining_indices,
                                stats= (players_stats, matches_elo_stats),
                                session = session,
                                start_from_checkpoint = start_from_checkpoint,
                                start = start
                            )
                    self.logger.info('Starting from Lookup, remaining pids: %s',len(remaining_pids))

                else:
                    # Collect N player_ids
                    indices_array, self.player_ids = self.client.collect_N()
                    indices_array_for_count = indices_array 
                        
                    await self.helper_supermatch(indices_array= indices_array, 
                                    player_ids= self.player_ids,
                                    count = indices_array_for_count,
                                    stats=(players_stats, matches_elo_stats),
                                    session = session,
                                    start_from_checkpoint = start_from_checkpoint,
                                    start = start)  
        
            except KeyboardInterrupt:
                self.logger.error(f"KeyboardInterrupt", exc_info= True)
                self.checkpointer()
                
            except Exception as e:
                self.logger.error(f"Error occured in supermatch: {e}", exc_info= True)
                self.checkpointer()

            except BaseException:
                self.checkpointer()
                raise BatchError(f"Batch failed processing at batch count: {self.batch_count}")
    

    async def helper_supermatch(self, indices_array, player_ids, count, stats, session, start_from_checkpoint, start):
        # accessing player ids for alter func

        # Async concurrent workers
        concurrent_workers = 6
        semaphore = asyncio.Semaphore(concurrent_workers)

        async def process_player(idx):
            async with semaphore:
                
                # 'success' acts as a flag and also contains alter_match_ids,
                # to be used by statistics functionlast
                self.logger.info(
                    f"({idx}) processing player id :{player_ids[idx]}"
                )  

                alter_match_ids, alter_data, alters = await self.client.alter_function(player_ids[idx], session)
                
                alter_match_ids =  list(set(alter_match_ids))
                n = len(alter_match_ids)

                # THIS IS SKIPPING THE PLAYER DUE TO LESS MATCHES
                if not alter_match_ids:
                    self.logger.warning(
                        f"({idx}) Player ID skipped due lesser number of matches in the past 40d!!"
                    )
                    return None

                # Processing each match id from the randomizer
                stats_tasks = [
                    #self.client.retry_function(
                        self.client.statistics_transform(match_id, session, idx)
                    #)
                    for match_id in alter_match_ids
                ]

                elo_tasks = [
                    #self.client.retry_function(
                        self.client.matches_elo(match_id, session, idx)
                    #)
                    for match_id in alter_match_ids
                ]
                
                local_concurrent_workers = 3
                local_semaphore = asyncio.Semaphore(local_concurrent_workers)

                async def throttle(coroutine):
                    async with local_semaphore:
                       return await coroutine

                results = await asyncio.gather(
                    *[throttle(t) for t in stats_tasks],
                    *[throttle(t) for t in elo_tasks],
                    self.client.lifetime_aggregates(player_ids[idx], session),
                    return_exceptions=True
                )

                stats_results = results[:n]
                elo_results = results[n:2*n]
                lifetime_result = results[-1]


                value_stats = [r for r in stats_results if r and not isinstance(r, Exception)]
                value_elo = [r for r in elo_results if r and not isinstance(r, Exception)]

                if start_from_checkpoint:
                    self.logger.info(
                        f"({idx}) Ran player id :{player_ids[idx]}"
                    )  
                else:
                    self.logger.info(
                        f"({idx+1}) Ran player id :{player_ids[idx]}"
                    )  
                
                result =  {
                    "idx": idx,
                    "stats": value_stats,
                    "elo": value_elo,
                    "lifetime": lifetime_result,
                    "matches": alter_data,
                    "alters": alters
                }

                await self.queue.put(result)
                self.last_checkpoint_upstream = player_ids[idx]
                
        async def run():
            
            # consumer
            consumer = asyncio.create_task(self.batch_processor())

            # producer
            tasks = [process_player(idx) for idx in indices_array]
            await asyncio.gather(*tasks)

            # block until 0 items in queue
            await self.queue.join()

            # consumer stopping
            consumer.cancel()

            # flushing the remaining batches
            # if self.batches:
            #     await self.batch_processor(flush = True)
            #     self.batches.clear()
                
            #     self.logger.info("Remaining batches flushed succesfully!!")

        await run()

        end = time.perf_counter()
        self.execution_time = end - start
        self.logger.info(f"Execution time : {self.execution_time:.4f} seconds")


    async def batch_processor(self): #flush = None        

        def store(batch):
            if batch.get('stats'):
                self.data.store_data(batch['stats'], 'ratings')

            if batch.get('elo'):
                self.data.store_data(batch['elo'], 'matches_elo')

            if batch.get('matches'):
                self.data.store_data(batch['matches'], 'matches')

            if batch.get('alters'):
                self.data.store_data(batch['alters'], 'alters')

            if isinstance(batch['lifetime'], dict):
                self.data.store_data(batch['lifetime'], 'lifetime')

        #if not flush:
        try:
            while True:
                item = await self.queue.get()
                self.batches.append(item)
                self.queue.task_done()
                if len(self.batches) >= self.batch_size:
                    for batch in self.batches:
                        if not batch:
                            continue

                        idx = batch['idx']
                        store(batch)
                        self.logger.info(f"Player ({idx}) stored to dB successfully!!")
                        #self.last_checkpoint_downstream = self.player_ids[idx]

                    self.batches.clear() 
                    self.batch_count += 1
                    self.logger.info(f"Batch Id ({self.batch_count}) has been successfully processed!!")

        except Exception as e:
            self.logger.error(f"batch_processor died: {e}", exc_info=True)

            # clearing dirty batch
            self.batches.clear()

        # else:
        #     for batch in local_batch:
        #         store(batch)
        #     self.batch_count += 1
        #     self.logger.info(f"Batch Id ({self.batch_count}) has been successfully processed!!")  
            

    def checkpointer(self):
        """
        
        checkpointer a wrapper for batch processing,
        and saves the state of a failed batch.
        """
        saves = {}
    
        self.logger.warning('checkpoint triggered due to sudden shutdown')
        self.logger.warning('saving recent checkpoint:\n @player_id %s \n @batch count %s', self.last_checkpoint_downstream, self.batch_count)
        
        zt_uae = ZoneInfo("Asia/Dubai")

        current_time = time.time()
        current_time_utc = datetime.datetime.fromtimestamp(current_time, datetime.timezone.utc)
        current_time_uae =  current_time_utc.astimezone(zt_uae)
        date = current_time_uae.strftime("%d %b %Y %I %M%p").replace(" ", "_")
        

        saves = {"time": str(current_time_uae),
                "last_player_id_upstream": self.last_checkpoint_upstream,
                "last_player_id_downstream": self.last_checkpoint_downstream,
                'batch_Id': self.batch_count}
        
        
        # request/response details
        self.runner_response_details = self.client.response_details
        
        if self.client.sum_of_latency == 0 or self.client.sum_of_requests == 0:
            rpm = 0
            avg_latency = 0
        else:
            rpm = (self.client.sum_of_requests/self.client.sum_of_latency + 1e-7) * 60
            avg_latency = self.client.sum_of_latency/self.client.sum_of_requests

        self.runner_response_details['stats'] = {
            "RPM": rpm,
            "avg_latency": avg_latency,
            'retries_count': self.client.retry_count,
            "total_execution_time": self.execution_time + self.client.fail_overall_total
        }



        with open(f"checkpoints/checkpoint_{date}.json", "w") as saves_json:
            json.dump(saves, saves_json, indent = 4)

        with open(f"response/response_{date}.json", "w") as response_json:
            json.dump(self.runner_response_details, response_json, indent = 4)
    
    def most_recent_checkpoint(self):
        """
        Performs aggregation lookup in MongoDB between lifetime and player collection to extract players which weren't processed by the program.
        
        """
        # folder = "checkpoints"

        # path = Path(folder)
        # file_pattern = "*.json"
        # files = list(path.glob(file_pattern))
    
        # if not files:
        #     self.logger.warning("No checkpoint found.")
        #     return 0

        # latest = max(files, key=os.path.getmtime)

        # with open(latest, 'r') as f:
        #     save = json.load(f)

        # player_id = save.get('last_player_id_downstream', 0)

        # if not player_id:
        #     return 0

        # self.logger.info("Retrieved most recent checkpoint: %s", player_id)

        lifetime_lookup = [
            {
            '$lookup': {
                'from': 'lifetime',
                'localField': '_id',
                'foreignField': '_id',
                'as': "lifetime_players"}
            },
            {
            '$match': {
                'lifetime_players': []}
            },
            
            {
                '$project': {'_id': 1}
            }
        ]

        filtered_cursor = self.data.players.aggregate(lifetime_lookup)
        filtered_players = list(filtered_cursor)
        print("Filtered players:",len(filtered_players))

        player_ids = [
            pid.get("_id") for pid in filtered_players
        ]
        indices_array = np.arange(len(player_ids))

        return indices_array, player_ids
        
        
       
    

            
            


        
        
        





