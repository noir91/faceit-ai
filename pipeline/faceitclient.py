import numpy as np
import os

from datetime import datetime, timedelta
import requests as r
import pandas as pd

from datetime import datetime, timedelta
import polars as pl
from functools import partial

import traceback
import time
from time import perf_counter
import logging
import asyncio
import aiohttp

import inspect

from configs.exceptions import SkippingMatch, SoftRateLimit, EmptyData
from polars.exceptions import OutOfBoundsError

class FaceitClient():

    """
    Faceit Client involves API endpoints and Methods 
    for Player Match processing from Faceit Data API

    This class is intended for Data Collection
    """

    def __init__(self, dbobj, headers):
        
        self.dbobj = dbobj
        self.headers = headers
        self.session = r.Session()
        self.error_flag = None
        self.logger = logging.getLogger(__name__)

        self.empty_count = 0
        self.skip_match = None

        self.rng = None

        self.start = True
        self.end = False

        # variables for storing status on API calls
        self.soft_rate = None
        self.sucess = None
        self.status = ""
        self.fail_overall_total = 0

        self.sum_of_requests = 0 
        self.sum_of_latency = 0
        self.retry_count = 0
        self.response_details = {}

        # Persistence 
        #self.stats_persistence = set()
        #self.elo_persistence = set()
        
        # if control stage is True, it's stage 1 and else stage 2
        self.control_stage = True

        # Network env variables
        self.fetch_calls = (self.fetch_statistics_transform, 
                            self.fetch_matches_elo, 
                            self.fetch_lifetime_url,
                            self.fetch_match)
        
    async def retry_function(self, function, *args, **kwargs):

        """
        Docstring for retry_function
        
        :param self: 
        :param function: an faceit api client function used for retrying
        """        
        response, data, status = None, [], None

        attempt = 0
        base_delay = 1
        retries = 5
        while True:

            try:
                if attempt > retries:
                    raise SkippingMatch("Max Retries Exceeded!!")
                
                else:
                    await asyncio.sleep(0.2)
                    response, data, status = await function(*args, **kwargs)
                    
                    # Guards
                    if status == 429:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=429,
                            message="rate limited",
                            headers=response.headers
                            )
                    
                    if data is None:
                        data = []
                        return response, data, status

                    if isinstance(data, (dict, list)) and len(data) == 0:
                        raise EmptyData("Fetched data from API is empty.")
                    
                    else:
                        return response, data, status

            except aiohttp.ClientResponseError as e:
                if e.status == 504:
                    sleep = 10
                    await asyncio.sleep(sleep)
                    self.logger.warning("Server failed to respond in time, retrying in %s", sleep )

                if e.status == 429:
                    attempt += 1
                    
                    retry_after = e.headers.get('Retry-After')
                    if retry_after:
                        await asyncio.sleep(int(retry_after))
                        self.logger.warning("Rate limited, sleeping %s seconds", retry_after)
                    else:
                        sleep_time = base_delay * (2** attempt)

                        await asyncio.sleep(sleep_time)
                        self.logger.warning("Rate limited, expbackoff %s seconds", sleep_time)
                else:
                    raise

                             
            except (r.exceptions.ConnectionError,
                    r.exceptions.Timeout,
                    r.exceptions.HTTPError,
                    ValueError) as e:
                attempt +=1
                sleep_time = base_delay * (2** attempt)

                self.logger.warning("Attempt %s\nRetrying function after %s", attempt, sleep_time)                
                self.logger.error("Error Type: %s",e)

                await asyncio.sleep(sleep_time)
                
            except SkippingMatch as e:
                self.skip_match = True
                self.logger.warning("Error on retry func: %s", e)
                data = []
                return response, data, status
            
            except EmptyData:
                self.logger.warning("Empty Data returned.")
                data = []
                return response, data, status

        
    async def alter_function(self, player_id, session):
        alter_match_ids, alter_data, alters = [], None, None
        try:
            result = await self.match(player_id, session, randomized = True)
    
            # Get Match and 10 player ids each
            alters, alter_data = result
            alter_match_ids = [item['_id'] for item in alter_data]

            if len(alters) < 15 or not alters:
                return alter_match_ids, alter_data, alters
            else:
            
            # Storing Alter raw match data from Faceit Data API
                self.dbobj.matches_batch.extend(alter_data)
                self.dbobj.alters_batch.extend(alters)

                return alter_match_ids, alter_data, alters
            
        except SkippingMatch as e:
            self.logger.warning("%s:  Alter Function didn't recieve a match", e)
            return alter_match_ids, alter_data, alters
        
    def collect_N(self):
        """
        collect_N retrieves 'N' player ids, stored in database.

        :param default: Default tells the function to run in its normal way and fetch seed set players which were
        used to expand the network. once it's false, it uses the alters not the seed set players, and then fetches matches.
        """
        # Fetch data from mongodb query
        players = self.dbobj.players
        players_id = [id['_id'] for id in players.find({}, {'_id': 1})]

        indices = {idx: value for idx, value in enumerate(players_id)}

        if not self.control_stage:
                
            alters = self.dbobj.alters
            alters_id = [
                pid
                for doc in alters.find({}, {"player_ids": 1, "_id": 0})
                for pid in doc.get("player_ids", [])
            ]

            filtered_alter_ids = []
            players_id = set(players_id)

            for alter in alters_id:
                if alter not in players_id:
                    filtered_alter_ids.append(alter)

            indices = {idx: value for idx, value in enumerate(filtered_alter_ids)}
            indices_array = np.array([i for i in indices.keys()])
            self.logger.info("Players retrieved from Collect N -> %s", len(indices_array))

            return indices_array, filtered_alter_ids
        
        else:
            indices = {idx: value for idx, value in enumerate(players_id)}
            indices_array = np.array([i for i in indices.keys()])

            self.logger.info("Players retrieved from Collect N -> %s", len(indices_array))
            return indices_array, players_id

    async def match(self, player_id, session, randomized = False):
        """
        Docstring for match
        
        :param player_id: Description
        :param headers: Description
        :param randomized: Flag used to decide wether we will randomize the matches and then fetch player_ids or not
        """
        # API endpoint
        history_url = f"https://open.faceit.com/data/v4/players/{player_id}/history"
        
        
        if self.control_stage:
            num_matches = 15
        else:
            num_matches = 5

        # Extracing UNIX timestamps (epochs)
        past_days =  int((datetime.now() - timedelta(days = 90)).timestamp())
        current_time = int(datetime.now().timestamp())

        match_ids = []
        player_ids = []
        all_data = []

        try:
            tasks = []
            
            for offset in range(0, 300, 100):
                # Query parameters
                params = {
                    "game": "cs2",
                    "from": past_days,
                    "to": current_time,
                    "offset": offset }
                
                tasks.append(
                    self.call_api(
                        url = history_url,
                        endpoint = 'match',
                        session = session,
                        params = params
                    )
                )

            results = await asyncio.gather(*tasks)
            
            for data, status, _ in results:
                if status == 200:
                    self.logger.info("GET status code: %s", status)

                    if "errors" in data:
                        self.logger.warning("Errors in data: %s", data["errors"])
                        continue
                    else:
                        for item in data.get("items", []):
                            match_id = item['match_id']
                            playing_players = item['playing_players']

                            match_ids.append(match_id)
                            player_ids.append(playing_players)

                            item['_id'] = item.pop('match_id')
                            all_data.append(item)
            
            # Randomizing Matches
            if randomized == True:

                # Sends a raise to alter function which then handles it as result = [], to skip player
                if match_ids and len(match_ids) < num_matches:
                    raise SkippingMatch("Less than 5 Matches Retrieved in Randomizer!")
                
                # lists to store ids
                alter_match_ids = []
                alter_ids = []

                #alter_data = []

                # calling randomized mataches func
                randomized_matches = set(self.match_randomizer(match_ids = match_ids,
                                num_matches= num_matches))
                
                # getting alter_match_ids from data.json()
                for single_match_data in all_data:
                    if single_match_data.get('_id') in randomized_matches:
                        # Extracting Match and Player ids, assigning a dictionary
                        alter_match_id = single_match_data['_id']
                        alter_match_ids.append(alter_match_id)

                        alter_id = single_match_data['playing_players']
                        alter_ids.append(alter_id)

                        # only raw data can pass from matches which are picked from randomized
                        # matches --> containing the alters
                
                alter_data = [index for index in all_data
                    if index.get('_id') in randomized_matches]

                # Making an alters output with unique match id and it's alters obtained
                alters = [
                    {"_id": alter_match_id, "player_ids": alter_id}
                    for alter_match_id, alter_id in zip(alter_match_ids, alter_ids)
                ]

                #print(len(alters))
                return alters, alter_data 
            else:

                return player_ids, data['items']
            
        except ConnectionError:
            raise ConnectionError
        
        except SoftRateLimit as e:
            print(f"Soft Rate Limit Hit! {e}")

        
    
    def match_randomizer(self, match_ids:list, num_matches):
        
        if not match_ids:
            raise SkippingMatch("There were no match IDs found! ")            

        # MongoDB checkup for existing data
        existing_matches = {doc['_id'] for doc in self.dbobj.matches.find({'_id': {'$in': match_ids}}, {'_id': 1})}
        filtered_indices = [mid for mid in match_ids if mid not in existing_matches]
        
        if not filtered_indices:
            raise SkippingMatch("All matches already exist in DB, nothing new to fetch.")
        
        # random sampling using numpy
        sample_size = min(num_matches, len(filtered_indices))

        default_rng = np.random.default_rng()
        sampled_ids = default_rng.choice(len(filtered_indices), size = sample_size, replace= False)

        randomized_matches = {filtered_indices[i] for i in sampled_ids}

        return randomized_matches

    def convert_json(self, incoming_json):
        """
    uses polars for faster conversion

    Converts expected incoming JSON (Array of objects for faction in teams from statistics for a match api endpoint)
    to Dataframe, computes the aggregate and then coverts it back to json.
        """
        try:
            data = (pl.DataFrame(incoming_json)
                    .cast(pl.Float32)
                    .mean())

            return data.row(0, named = True)
        
        except (OutOfBoundsError) as e:
            raise SkippingMatch("Failed to find aggregates of an empty sequence!") 
            
    async def statistics_transform(self, match_id, session, count):
        '''
        Docstring for statistics transform
        
        Aggregate Team Function aggregates scores of 2 Factions (teams) within a match, a component of the pipeline 
        in delivering statistics to the database for collection
        
        The statistics stay as they are, just a new key is added per
        faction for the aggregates.

        Returns the statistics of a match for all players, will be used 
        to store data into 'ratings' collection.
        '''
        statistics_url = f"https://open.faceit.com/data/v4/matches/{match_id}/stats"

        
        #response = r.get(statistics_url, headers = self.headers, timeout = 3)
        #match_data = response.json()

       # if match_id not in self.stats_persistence:
        match_data, _, latency = await self.call_api(url= statistics_url, endpoint = 'statistics', session = session, count= count)
            #match_data = await response.json()
            #self.stats_persistence.add(match_id)

        #else:
            #self.logger.info("statistics are persistence, not calling API.")
            #return None
        
        #if match_id in self.stats_persistence:
           # self.retry_function(self.matches_elo, match_id)

        # Temporary store for all player statistics dictionaries
        faction1 = []
        faction2 = []
        players_list = []
        statistics = {}

        

        try:
            rounds_data = match_data.get("rounds")
            
            if not rounds_data:
                self.skip_match = True
                await self.detect_soft_rate_limit(data = None, skip = True)
                
                self.trigger_response(f"stats/{match_id}", latency, status_override= 'skip_match' )
                self.logger.warning(f"Skipping match: {match_id} - Failure to get statistics for game.")
                
                raise SkippingMatch("Skipping match")
            
            self.skip_match = False
            self.logger.info("Statistics found for game %s", match_id)

            for rounds in rounds_data:
                for index, teams in enumerate(rounds.get("teams")):
                    for team_players in teams.get('players'):
                        
                        # Removes Team Name which is in string, data coming from API
                        if ('Team' in teams['team_stats']) == True:
                            del teams['team_stats']['Team']

                        del team_players['nickname']
                        if index == 0:
                            # Team(s) stats
                            faction1_stats = teams['team_stats']

                            # Player(s) stats
                            #individual_stats_1 = team_players
                            faction1.append(team_players['player_stats'])
                        else:
                            # Team(s) stats
                            faction2_stats = teams['team_stats']
    
                            # Player(s) stats
                            #individual_stats_2 = team_players

                            faction2.append(team_players['player_stats'])
                            
                        players_list.append(team_players)

            # Team(s) aggregates
            faction1_agg = self.convert_json(faction1)
            faction1_agg.update(faction1_stats)
            #faction1_agg.update(individual_stats_1)

            faction2_agg = self.convert_json(faction2)
            faction2_agg.update(faction2_stats)
            #faction1_agg.update(individual_stats_2)


            agg = {
                "faction1": faction1_agg,
                "faction2": faction2_agg
            }

            statistics['_id'] = match_id
            statistics['players'] = players_list
            statistics['team_agg'] = agg
            statistics['stageId'] = "stage_1_"+str(datetime.now().strftime("%Y%m%d_%H%M")) if self.control_stage else "stage_2_"+str(datetime.now().strftime("%Y%m%d_%H%M"))

            return statistics
        
        except ConnectionError:
            raise ConnectionError

        except SoftRateLimit as e:
            self.logger.warning("Soft Rate Limit Hit! %s", e)

    def retrieve_hub_members(self, hub_id):
        """
        Fetch function, Fetches the Member's nickname from 
        Faceit API for processing.
        
        """
        
        try:
            url = f"https://open.faceit.com/data/v4/hubs/{hub_id}/members"
            
            players_list = []
            keep = ['user_id', 'nickname', 'faceit_url']
            
            # Pagination loop

            offset = 0
            limit = 50
        
            while True:
                params = {'offset': offset,
                        'limit': limit}
                
                response, _= self.call_api(url = url, params =params)
                data = response.json()

                if not data:
                    self.detect_soft_rate_limit(data)
                
                else: 
                    offset += limit
                    for k in data:
                        if response.status_code == 200:
                            self.logger.info("GET", url, 'status code:', response.status_code)   
                            
                            if k == 'start':
                                break
                            for dic in data[k]: # user dictionaries
                                for k, v in dic.items(): # user diciontary
                                    if k in keep:
                                        temps = {('_id' if k == 'user_id' else k): v for k, v in dic.items() if k in keep}
                                
                                players_list.append(temps)
                        else:
                            self.logger.error(f"Request Error {response.status_code},\n {response.json()['errors']}")
                            break
                    
                    if 'items' not in data:
                        csv = pd.DataFrame(players_list)
                        csv.to_csv('checkpoint_members.csv')
                        self.logger.info(f"Checkpoint saved as csv: {os.getcwd()}")
                        break

                return players_list
            
        except SoftRateLimit as e:
            print(f"Soft Rate Limit Hit! {e}")

    def retrieve_ID_members(self, nicknames: list[str], game = 'cs2', status = True):
        """
        Function retrieves ID(s) of members.
        
        where, 
            nicknames --> list

        """
        try:
            url = "https://open.faceit.com/data/v4/players"

            id_list = []

            for nickname in nicknames:
                params = {
                        'nickname': nickname,
                        'game': game}
                
                response, _ = self.call_api(url = url, params =params)
                
                if status == True:
                    self.logger.info(f"retrieve_ID_members status code: {response.status_code}")

                data = response.json()
                if not data:
                    self.detect_soft_rate_limit(data)
                    
                for i in data:
                    if i == 'player_id':
                        player_id = data[i]
                    else:
                        break
                    
                id_list.append(player_id)


            return id_list
        
        except SoftRateLimit as e:
            print(f"Soft Rate Limit Hit! {e}")

    async def lifetime_aggregates(self, player_id, session):

        """
        Historical aggregates will have a player's historical map pool statistics providing for good features.
        This will include individual statistics, and map specific winrates.

        soon :their recent 2 week performance aggregates.

        """

        try:
            game_id = 'cs2'

            lifetime_url = f"https://open.faceit.com/data/v4/players/{player_id}/stats/{game_id}"
            lifetime_data,_ , _= await self.call_api(url= lifetime_url, endpoint = 'lifetime', session = session)
            #lifetime_data = await response.json()

            if not lifetime_data:
                await self.detect_soft_rate_limit(lifetime_data)
            
            else:
                for segments in lifetime_data.get('segments', []):
                        for key in list(segments.keys()):

                            if "img" in key:
                                del segments[key]      

                # renaming player_id with _id for mongodb unique id
                lifetime_data['_id'] = lifetime_data.pop('player_id')
                lifetime_data['stageId'] = "stage_1_"+str(datetime.now().strftime("%Y%m%d_%H%M")) if self.control_stage else "stage_2_"+str(datetime.now().strftime("%Y%m%d_%H%M"))

                return lifetime_data
        
        except (ConnectionError,
                TimeoutError) as e:
            print(f"Connection interrupted. {e}")

        except SoftRateLimit as e:
            print(f"Soft Rate Limit Hit! {e}")
        
        except Exception as e:
            print(f"Error in lifetime aggregates : {e}")


    async def matches_elo(self, match_id, session, count):
        
        """
        :param count: used to indicate which player we are iterating through using count
        """
        matches_elo_url = f"https://open.faceit.com/data/v4/matches/{match_id}"
        
        match, _, latency = await self.call_api(url= matches_elo_url, endpoint = 'elo', session =session, count = count)
        #match = await response.json()
        
        self.logger.info('Matches elo API endpoint called for %s', match_id)
        
        try:    

            if not match:
                self.skip_match = True
                await self.detect_soft_rate_limit(data = None, skip = True)

                self.trigger_response(f"matches/{match_id}", latency, status_override= 'skip_match')
                self.logger.warning("Skipping match: %s - Failure to get matches elo for game.", match_id)

                raise SkippingMatch("Skipping match's elo")
            
            self.skip_match = False
            for key in list(match.keys()):
                if key not in {
                    "match_id",
                    "started_at",
                    "finished_at",
                    "teams",
                    "results",
                    "detailed_results",
                    "voting"
                }:
                    del match[key]

            if "voting" in match:
                voting = match["voting"]

                for key in list(voting.keys()):
                    if key != "map":
                        del voting[key]

                if "map" in voting:
                    map_block = voting["map"]
                    for key in list(map_block.keys()):
                        if key != "pick":
                            del map_block[key]

            if "teams" in match:
                for faction in ("faction1", "faction2"):
                    if faction not in match["teams"]:
                        continue

                    team = match["teams"][faction]

                    # keep only roster + stats.rating
                    for key in list(team.keys()):
                        if key not in {"roster", "stats"}:
                            del team[key]

                    if "stats" in team:
                        for key in list(team["stats"].keys()):
                            if key != "rating":
                                del team["stats"][key]

                    if "roster" in team:
                        for player in team["roster"]:
                            for key in list(player.keys()):
                                if key not in {
                                    "player_id",
                                    "game_player_id",
                                    "game_skill_level"
                                }:
                                    del player[key]
            match['_id'] = match_id
            match['stageId'] = "stage_1_"+str(datetime.now().strftime("%Y%m%d_%H%M")) if self.control_stage else "stage_2_"+str(datetime.now().strftime("%Y%m%d_%H%M"))

            return match
        
        except ConnectionError:
            raise ConnectionError

        except SoftRateLimit as e:
            self.logger.warning('Soft Rate Limit Hit! %s', e)

    async def detect_soft_rate_limit(self, data, skip = False):

        empty_count = 0
        #if not self.skip_match:
        if skip or not data or data == []:
            empty_count += 1
            self.status = 'skip_match' if skip else "rate_limited"
        else:
            empty_count = 0
            self.status = 'success'

        if empty_count >= 3: 
            self.status = 'rate_limited'
            raise SoftRateLimit

    async def call_api(self, url, endpoint= None, session = None, params = None, count = None):

        """
        Runs and Records API connection information for testing and optimizing the client for performance.
        
        :param url: request from url
        :param endpoint: points to which fetch api function to run
        :param session: aiohttp client session object
        :param params: query parameters for the call
        :param match_func: matches_func refers to match() function, if call_api is used in it, async wont be used on it.
        :param count: count is used for tracking the iterating players.
        """
        start = perf_counter()
        latency = 0
        endpoints = {'statistics': self.fetch_calls[0], 
                     'elo': self.fetch_calls[1],
                     'lifetime': self.fetch_calls[2],
                     'match': self.fetch_calls[3]}
        try:            
            # Match Endpoint logic
            #if match_func:
            #    start = perf_counter()
            #    a
            #    response = self.session.get(url = url, params = params,
            #                    headers = self.headers, timeout =3)
            #    data = response.json()
            # All other endpoints
            #else:
            func = endpoints.get(endpoint)

            if not endpoint:
                raise ValueError(f"Invalid endpoint : {endpoint}")
            _, data, status = await self.retry_function(func, url =url, session = session, params = params, count = count)

            #data = await response.json()
            
            latency = perf_counter() - start

            # Data check and status logic
            #if match_func:
            #    if response.status_code == 429:
            #        self.status = 'rate_limited'
            #        raise SoftRateLimit("HTTP 429 Error")
            #else:
            #if response.status == 429:
            if isinstance(data, dict) and 'errors' in data:
                self.status = 'rate_limited'
                #raise SoftRateLimit("HTTP 429 Error")
                
            if endpoint and 'stats' not in endpoint:
                if isinstance(data, dict) and "errors" in data:
                    self.status = 'rate_limited'
                    #raise SoftRateLimit(data['errors'])
            
            await self.detect_soft_rate_limit(data)

            return data, status, latency

        finally:
            self.sum_of_requests += 1
            self.sum_of_latency += latency
            
            self.logger.debug("ENDPOINT: %s, LATENCY %s", endpoint, latency)
            
            self.trigger_response(endpoint, latency)
    
    def trigger_response(self, endpoint, latency, status_override = None):
        
        status_to_log = status_override or self.status
        self.len_resp = len(self.response_details)
        self.response_details[f"{self.len_resp + 1}"] = {
                "url": endpoint,
                "latency": latency,
                "status": status_to_log
            }
    

        self.logger.info("Response Latency: %.3f, Status: %s            %s", latency, status_to_log, endpoint)

    async def fetch_statistics_transform(self, url, session, params = None, count = None):
        """gets statistics in detail from statistics endpoint, returns json object"""

        self.logger.info("(%s) FETCH statistics -> %s", count, url)
        async with session.get(url, headers = self.headers, params = params) as response:
            data = await response.json()
            status = response.status
            self.logger.info("(%s) DONE  statistics -> status:%s", count, status)
            return response, data, status
        

    async def fetch_matches_elo(self, url, session, params = None, count = None):
        """gets elo in detail from matches_elo endpoint, returns json object"""

        self.logger.info("(%s) FETCH elo -> %s", count, url)
        async with session.get(url, headers =self.headers, params = params) as response:
            data = await response.json()
            status = response.status
            self.logger.info("(%s) DONE  elo -> status:%s", count, status)     
            return response, data, status

    async def fetch_lifetime_url(self, url, session, params= None, count = None):
        """gets lifetime in detail from lifetime endpoint, returns json object"""

        self.logger.info("FETCH lifetime -> %s", url)
        async with session.get(url, headers = self.headers, params = params) as response:
            data = await response.json()
            status = response.status
            self.logger.info("DONE  lifetime -> status:%s", status)
            return response, data, status

    async def fetch_match(self, url, session, params= None, count = None):
        """gets matches in detail from match endpoint, returns json object"""
        
        self.logger.info("FETCH match -> %s", url)
        async with session.get(url, headers = self.headers, params = params) as response:
            data = await response.json()
            status = response.status
            self.logger.info("DONE  match -> status:%s", status)
            return response, data, status

