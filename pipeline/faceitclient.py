import numpy as np
import os
from datetime import datetime, timedelta
import requests as r
import pandas as pd
import os
from datetime import datetime, timedelta
import polars as pl


class FaceitClient():

    """
    Faceit Client involves API endpoints and Methods 
    for Player Match processing from Faceit Data API

    This class is intended for Data Collection
    """

    def __init__(self, dbobj, headers):

        self.dbobj = dbobj
        self.headers = headers

    def alter_function(self, player_id):

        print("-"*20,"Retrieving Alters and their matches:","-"*20)

        # Get Match and 10 player ids each
        alters, alter_data = self.match(player_id= player_id,
                                                headers= self.headers, randomized= True)

        # Store player ids in mongodb 

        # Control structure to remove any N player_ids which don't have 10 matches
        # in the past 40 days
        
        if len(alters) < 15:
            return False
        else:
        
        # Storing Alter raw match data from Faceit Data API        
            self.dbobj.store_data(batch = alter_data, 
                            collection = 'matches', 
                            verbose = False)

        
            self.dbobj.store_data(batch = alters, 
                            collection = 'alters', 
                            verbose = True)
            return True    

    def collect_N(self):
        """
        collect_N retrieves 'N' player ids, stored in database.
        """

        # Fetch data from mongodb query
        players = self.dbobj.players
        players_id = [id['_id'] for id in players.find({}, {'_id': 1})]

        return players_id

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

        while True:
    
            # Query parameters
            params = {
                "game": "cs2",
                "from": past_40_days,
                "to": current_time,
                "offset": offset,
                "limit": limit}

            history_response = r.get(url = history_url,
                                    headers = self.headers,
                                    params = params)
        
            data = history_response.json()
            offset += limit        

            if history_response.status_code == 200:
                print("GET", "status code: ", history_response.status_code)
                for item in data.get("items", []):
                    match_id = item['match_id']
                    playing_players = item['playing_players']

                    match_ids.append(match_id)
                    player_ids.append(playing_players)

                    item['_id'] = item.pop('match_id')
                    all_data.append(item)
                
            if offset >= 200:
                break

        # Random Sampling -->
            # Randomizing Matches
        if randomized == True:
            
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
                    # Extracting Match and Player Ids, assigning a dictionary
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
            print(len(alters))
            return alters, alter_data 
        else:

            return player_ids, data['items']
        
    
    def match_randomizer(self, match_ids:list, seed = 42):
        
        # Randomized Match Ids
        randomized_matches = set()

        # Reproducibility
        np.random.seed(seed = seed)

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

    def convert_json(self, inc_json):
        """
    uses polars for faster conversion

    Converts expected incoming JSON (Array of objects for faction in teams from statistics for a match api endpoint)
    to Dataframe, computes the aggregate and then coverts it back to json.
        """
        data = (pl.DataFrame(inc_json)
                .cast(pl.Float32)
                .mean())

        return data.row(0, named = True)

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

        response = r.get(statistics_url, headers = self.headers)
        match_data = response.json()
        print(match_data)

        # Temporary store for all player statistics dictionaries
        faction1 = []
        faction2 = []
        players_list = []
        statistics = {}

        for rounds in match_data.get("rounds"):
            for index, teams in enumerate(rounds.get("teams")):
                for team_players in teams.get('players'):
                    if ('Team' in teams['team_stats']) == True:
                        del teams['team_stats']['Team']

                    del team_players['nickname']
                    if index == 0:
                        # Team(s) stats
                        faction1_stats = teams['team_stats']

                        # Player(s) stats
                        faction1.append(team_players['player_stats'])
                    else:
                        # Team(s) stats
                        faction2_stats = teams['team_stats']
 
                        # Player(s) stats
                        faction2.append(team_players['player_stats'])
                        
                    players_list.append(team_players['player_id'])

        # Team(s) aggregates
        faction1_agg = self.convert_json(faction1)
        faction1_agg.update(faction1_stats)

        faction2_agg = self.convert_json(faction1)
        faction2_agg.update(faction2_stats)

        agg = {
            "faction1": faction1_agg,
            "faction2": faction2_agg
        }

        statistics['_id'] = match_id
        statistics['players'] = players_list
        statistics['team_agg'] = agg

        return statistics

    def retrieve_hub_members(self, hub_id):
        """
        Fetch function, Fetches the Member's nickname from 
        Faceit API for processing.
        
        """
        
        url = f"https://open.faceit.com/data/v4/hubs/{hub_id}/members"
        
        players_list = []
        keep = ['user_id', 'nickname', 'faceit_url']
        
        # Pagination loop

        offset = 0
        limit = 50

        while True:
            params = {'offset': offset,
                    'limit': limit}
            
            request = r.get(url = url,
                            headers = self.headers,
                            params= params)
            
            data = request.json()

            offset += limit
            for k in data:
                if request.status_code == 200:
                    print("GET", url, 'status code:', request.status_code)   
                    
                    if k == 'start':
                        break
                    for dic in data[k]: # user dictionaries
                        for k, v in dic.items(): # user diciontary
                            if k in keep:
                                temps = {('_id' if k == 'user_id' else k): v for k, v in dic.items() if k in keep}
                        
                        players_list.append(temps)
                else:
                    print(f"Request Error {request.status_code},\n {request.json()['errors']}")
                    break
            
            if 'items' not in data:
                csv = pd.DataFrame(players_list)
                csv.to_csv('checkpoint_members.csv')
                print(f"Checkpoint saved as csv: {os.getcwd()}")
                break

        return players_list

    def retrieve_ID_members(self, nicknames: list[str], game = 'cs2', status = True):
        """
        Function retrieves ID(s) of members.
        
        where, 
            nicknames --> list

        """
        url = "https://open.faceit.com/data/v4/players"

        id_list = []

        for nickname in nicknames:
            params = {
                    'nickname': nickname,
                    'game': game}

            ID_request = r.get(url = url,
                            headers = self.headers,
                            params = params)
            if status == True:
                print(ID_request.status_code)

            data = ID_request.json()
            if not data:
                break
            for i in data:
                if i == 'player_id':
                    player_id = data[i]
                else:
                    break
                
            id_list.append(player_id)


        return id_list