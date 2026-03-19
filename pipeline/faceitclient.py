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
        self.stats_persistence = set()
        self.elo_persistence = set()
        


    def retry_function(self, function, *args, **kwargs):

        """
        Docstring for retry_function
        
        :param self: 
        :param function: an faceit api client function used for retrying
        """
        overall_start = perf_counter()

        attempt = 0
        base_delay = 1
        while True:

            try:
                retries = 5
                attempt += 1
                if attempt > retries:
                    overall_end = perf_counter()
                    self.fail_overall_total = overall_end - overall_start
                    
                    self.logger.info("Total time FAILED: %s", self.fail_overall_total)
                    raise Exception("Max Retries Exceeded!!")
                else:
                    
                    result = function(*args, **kwargs)
                    
                    # Guards
                    if result is None:
                        return None

                    if isinstance(result, list) and len(result) == 0:
                        raise EmptyData("Fetched data from API is empty.")
                    
                    else:
                        return result


            except (r.exceptions.ConnectionError, 
                    r.exceptions.Timeout, 
                    r.exceptions.HTTPError, 
                    ValueError) as e:
                
                self.retry_count +=1
                sleep_time = base_delay * (3 ** attempt)

                self.logger.warning("Attempt %s\nRetrying function after %s", attempt, sleep_time)                
                self.logger.error("Error Type: %s",e)

                time.sleep(sleep_time)

            except SkippingMatch as e:
                self.skip_match = True
                self.logger.warning("Error on retry func: %s", e)
                return None
            
            except EmptyData:
                self.logger.warning("Empty Data returned.")
                return None



         #except Exception as e:
        #    print(f"Error : {e}")
        #    traceback.print_exc()

        #except KeyboardInterrupt:
        #    print("\nStopped by user.")
        
        #except TypeError as te:
        #    print(f"\nTypeError: {te}")
        
    def alter_function(self, player_id):
        
        try:
            self.logger.info("%s Retrieving Alters and their matches %s", "-"*20, "-"*20)

            result = self.retry_function(self.match, player_id, randomized = True)
            if result is None:
                return []
            
            # Get Match and 10 player ids each
            alters, alter_data = result
            
            alter_match_ids = [item['_id'] for item in alter_data]

            # Store player ids in mongodb 

            # Control structure to remove any N player_ids which don't have 10 matches
            # in the past 40 days
            
            # IF LESS MATCHES THAN 15 OR EMPTY ALTERS, we return []
            if len(alters) < 15 or not alters:
                return []
            
            else:
            
            # Storing Alter raw match data from Faceit Data API
                self.dbobj.matches_batch.extend(alter_data)
                self.dbobj.alters_batch.extend(alters)

                return alter_match_ids 
            
        except SkippingMatch as e:
            self.logger.warning("Alter Function didn't recieve a match: %s",e)
            return []
        
    def collect_N(self):
        """
        collect_N retrieves 'N' player ids, stored in database.
        """

        # Fetch data from mongodb query
        players = self.dbobj.players
        players_id = [id['_id'] for id in players.find({}, {'_id': 1})]

        indices = {idx: value for idx, value in enumerate(players_id)}   
        indices_array = np.array([i for i in indices.keys()])
        
        return indices_array, players_id

    def match(self, player_id, randomized = False):
        """
        Docstring for match
        
        :param player_id: Description
        :param headers: Description
        :param randomized: Flag used to decide wether we will randomize the matches and then fetch player_ids or not
        """
        # API endpoint
        history_url = f"https://open.faceit.com/data/v4/players/{player_id}/history"
        
        # Extracing UNIX timestamps (epochs)
        past_40_days =  datetime.now() - timedelta(days = 40)
        current_time = datetime.now()

        offset = 0
        limit = 100

        match_ids = []
        player_ids = []
        all_data = []

        try:
            while True:
        
                # Query parameters
                params = {
                    "game": "cs2",
                    "from": past_40_days,
                    "to": current_time,
                    "offset": offset,
                    "limit": limit}
                
                #history_response = r.get(url = history_url,
                #                        headers = self.headers,
                #                        params = params,
                #                        timeout = 3)
                

                #data = history_response.json()

                history_response, _= self.call_api(endpoint= history_url, params = params)
                data = history_response.json()

                offset += limit        
                #if not history_response:
                #    self.detect_soft_rate_limit(history_response)
                    
                #else:
            
                if history_response.status_code == 200:
                    self.logger.info("GET status code: %s", history_response.status_code)
                    for item in data.get("items", []):
                        match_id = item['match_id']
                        playing_players = item['playing_players']

                        match_ids.append(match_id)
                        player_ids.append(playing_players)

                        item['_id'] = item.pop('match_id')
                        all_data.append(item)
                
                else:
                    history_response.raise_for_status()
                    self.error_flag = True

                if offset >= 200:
                    break

            # Random Sampling -->
                # Randomizing Matches
            if randomized == True:

                # Sends a raise to alter function which then handles it as result = [], to skip player
                if match_ids and len(match_ids) <15:
                    raise SkippingMatch("Less than 15 Matches Retrieved in Randomizer!")
                
                # lists to store ids
                alter_match_ids = []
                alter_ids = []

                #alter_data = []

                # calling randomized mataches func
                randomized_matches = set(self.match_randomizer(match_ids = match_ids,
                                seed = 42))
                
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

        
    
    def match_randomizer(self, match_ids:list, seed = 42):
        
        if not match_ids:
            raise SkippingMatch("There were no match IDs found! ")            
        
        # Randomized Match Ids
        randomized_matches = set()

        # Reproducibility
        self.rng = np.random.seed(seed = seed)

        # Storing enumerations as indices to sample
        indices = {i: item for i, item in enumerate(match_ids)}   
        indices_array = np.array([i for i in indices.keys()])

        # random sampling using numpy
        rng = np.random.choice(indices_array,
                            size = 15, # 10 Matches from past 40 days
                            replace = False # Sampling without replacement
                            )
        
        for k in indices.keys():
            if k in rng:
                randomized_matches.add(indices[k])
            else:
                continue

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
            
    def statistics_transform(self, match_id):
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

        if match_id not in self.stats_persistence:
            response, latency = self.call_api(endpoint= statistics_url)
            match_data = response.json()
            self.stats_persistence.add(match_id)

        else:
            self.logger.info("statistics are persistence, not calling API.")
            return []
        
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
                self.detect_soft_rate_limit(data = None, skip = True)
                
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

                    
                #request = r.get(url = url,
                #                headers = self.headers,
                #                params= params,
                #                timeout = 3
                #                )
                 

                #data = request.json()
                
                response, _= self.call_api(endpoint = url, params =params)
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
                
                #ID_request = r.get(url = url,
                #                headers = self.headers,
                #                params = params,
                #               timeout = 3)

                response, _ = self.call_api(endpoint = url, params =params)
                
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

    def lifetime_aggregates(self, player_id):

        """
        Historical aggregates will have a player's historical map pool statistics providing for good features.
        This will include individual statistics, and map specific winrates.

        soon :their recent 2 week performance aggregates.

        """

        try:
            game_id = 'cs2'

            lifetime_url = f"https://open.faceit.com/data/v4/players/{player_id}/stats/{game_id}"
            
            #resp = r.get(
            #    url = lifetime_url,
            #    headers = self.headers
            #)
             
            #lifetime_data = resp.json()
            
            response, _= self.call_api(endpoint= lifetime_url)
            lifetime_data = response.json()

            if not lifetime_data:
                self.detect_soft_rate_limit(lifetime_data)
            
            else:
                for segments in lifetime_data.get('segments', []):
                        for key in list(segments.keys()):

                            if "img" in key:
                                del segments[key]      

                # renaming player_id with _id for mongodb unique id
                lifetime_data['_id'] = lifetime_data.pop('player_id')
                
                return lifetime_data
        
        except (ConnectionError,
                TimeoutError) as e:
            print(f"Connection interrupted. {e}")

        except SoftRateLimit as e:
            print(f"Soft Rate Limit Hit! {e}")
        
        except Exception as e:
            print(f"Error in lifetime aggregates : {e}")


    def matches_elo(self, match_id):
        
        matches_elo_url = f"https://open.faceit.com/data/v4/matches/{match_id}"
        
        response, latency = self.call_api(endpoint= matches_elo_url)
        match = response.json()

        self.logger.info('Matches elo API endpoint called for %s', match_id)
        
        try:    

            if not match:
                self.skip_match = True
                self.detect_soft_rate_limit(data = None, skip = True)

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

            return match
        
        except ConnectionError:
            raise ConnectionError

        except SoftRateLimit as e:
            self.logger.warning('Soft Rate Limit Hit! %s', e)

    def detect_soft_rate_limit(self, data, skip = False):
        
        #if not self.skip_match:
        if skip or not data or data == []:
            self.empty_count += 1
            self.status = 'skip_match' if skip else "rate_limited"
        else:
            self.empty_count = 0
            self.status = 'success'

        if self.empty_count >= 3: 
            self.status = 'rate_limited'
            raise SoftRateLimit
            


    def call_api(self, endpoint, params = None, session = True):

        """
        Records API connection information for testing and optimizing the client for performance.
        
        :param flag: indicative of starting or stopping a timer for latency 
        :param session: session object
        """
        latency = 0

        try:
            if session:
                
                start = perf_counter()
                response = self.session.get(url = endpoint, params = params,
                                headers = self.headers, timeout =3)

                end = perf_counter()
                
            else:
                start = perf_counter()
                response = r.get(url = endpoint, params = params,
                                headers = self.headers, timeout =3)

                end = perf_counter()
        
            data = response.json()
            latency = end - start

            if response.status_code == 429:
                self.status = 'rate_limited'
                raise SoftRateLimit("HTTP 429 Error")
            
            if 'stats' not in endpoint:
                if isinstance(data, dict) and "errors" in data:
                    self.status = 'rate_limited'
                    raise SoftRateLimit(data['errors'])
            
            self.detect_soft_rate_limit(data)

            return response, latency

        finally:
            self.sum_of_requests += 1
            self.sum_of_latency += latency
            
            self.logger.debug("ENDPOINT: %s, LATENCY %s", endpoint, latency)
            
            self.trigger_response(endpoint, latency)

        # FIX RATE LIMIT NOT SHOWING IN RESPONSE LOGS 
    
    def trigger_response(self, endpoint, latency, status_override = None):
        
        status_to_log = status_override or self.status
        self.len_resp = len(self.response_details)
        self.response_details[f"{self.len_resp + 1}"] = {
                "url": endpoint,
                "latency": latency,
                "status": status_to_log
            }
    

        self.logger.info("Response Latency: %.3f, Status: %s            %s", latency, status_to_log, endpoint)