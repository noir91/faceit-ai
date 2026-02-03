import requests as r
import pandas as pd
import os
from pipeline.orch import getdata
from datetime import datetime, timedelta
import numpy as np
import sys
import time


def retrieve_hub_members(headers, hub_id):
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
                        headers = headers,
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

def retrieve_ID_members(headers, nicknames: list[str], game = 'cs2', status = True):
    """
    Function retrieves ID(s) of members.
    
    where, 
        nicknames --> list

    """
    url = "https://open.faceit.com/data/v4/players"

    id_list = []

    while True:
        for nickname in nicknames:
            params = {
                    'nickname': nickname,
                    'game': game}

            ID_request = r.get(url = url,
                            headers = headers,
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

def alter_function(player_id, headers, data):

    print("-"*20,"Retrieving Alters and their matches:","-"*20)

    # Get Match and 10 player ids each
    alters, alter_data = match(player_id= player_id,
                                             headers= headers, randomized= True)

    # Store player ids in mongodb 

    # Control structure to remove any N player_ids which don't have 10 matches
    # in the past 40 days
    
    if len(alters) < 15:
        return False
    else:
        #print(f"Preview of data : \n {pd.DataFrame(alter_data)} \n{pd.DataFrame(alters)}")
    
    #print(f"raw data: \n{alters}" )
    #if prompt in ['yes','y']:

    # Storing Alter raw match data from Faceit Data API
    #    if len(alter_data) != 0:
    
        data.store_data(batch = alter_data, 
                        collection = 'matches', 
                        verbose = False)

    #if len(alters) != 0:
    
        data.store_data(batch = alters, 
                        collection = 'alters', 
                        verbose = True)
        return True    


    #elif prompt in ['no', 'n']:
    #    sys.exit()
    #else:
    #    prompt = input("Enter the correct response (Y/N): ")

def match_randomizer(match_ids:list, seed = 42):
    
    #print("(DEBUG) Entering match randomzier func!!!")
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
            
def collect_N(data):
    """
    collect_N retrieves 'N' player ids, stored in database.
    """

    # Fetch data from mongodb query
    players = data.players
    players_id = [id['_id'] for id in players.find({}, {'_id': 1})]

    return players_id

def supermatch(headers, host, port):
    # Connecting MongoDB 
    data = getdata()
    data.connect_db(host = host, port = port)
    
    # Collect N player_ids
    player_ids = collect_N(data = data)
    #player_ids = ['22531ed3-e6f6-4647-932f-52fd8af17073']

    # Get 10 randomized matches and their alters

    # accessing player ids for alter func
    for i in range(len(player_ids)):
        success = alter_function(player_id = player_ids[i],
                       headers = headers,
                       data = data)
        if success == True:
            print(f"({i+1}) Ran player id :{player_ids[i]}, alters and matches stored!")
        else: 
            print(f"({i+1}) Player ID skipped due lesser number of matches in the past 40d!!")
        time.sleep(0.7)

    # Get statistics of all the existing matches

    # Fetching match_ids query
    matches = data.matches
    match_ids = [id['_id'] for id in matches.find({}, {'_id': 1})]
    players_stats = []

        # I THINK I SHOULD MOVE THIS TO MAIN SCRIPT
    # Storing statistics of each player
    for match_id in match_ids:
        statistics = statistics_match_func(match_id= match_id, 
                                           headers= headers,
                                           data = data)
        players_stats.append(statistics)
        data.store_data(batch = players_stats,
                        collection = 'ratings')
        
# players list fetching from randomized matches should be a different function
def statistics_match_func(match_id, headers, data):
    """
    Returns the statistics of a match for all players, will be used 
    to store data into 'ratings' collection.
    """

    statistics_url = f"https://open.faceit.com/data/v4/matches/{match_id}/stats"

    response = r.get(statistics_url, headers = headers)
    data = response.json()

    statistics = {}
    players_list = []

    for item in data.get('rounds', []):
        for teams in item.get('teams', []):
            for players in teams.get('players'):
                players_list.append(players)

        statistics['_id'] = match_id
        statistics['players'] = players_list
    
    return statistics


def match(player_id, headers, randomized = False):
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
                                 headers = headers,
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
        randomized_matches = set(match_randomizer(match_ids = match_ids,
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

        #alter_data = [ item['_id']: item 
        #              for item in all_data.get('items', []) 
        #              if item.get('_id') in randomized_matches
        #              ]
        
        alter_data = [index for index in all_data
            if index.get('_id') in randomized_matches]
        
        #alter_data = [item
         #       for item in all_data.get('items', [])
        #        if item['_id'] in randomized_matches
        #   ]

        # Making an alters output with unique match id and it's alters obtained
        alters = [
            {"_id": alter_match_id, "player_ids": alter_id}
            for alter_match_id, alter_id in zip(alter_match_ids, alter_ids)
        ]
        print(len(alters))
        return alters, alter_data 
    else:
        return player_ids, data['items']

def agg_team_func(match_data):
    '''
    Docstring for agg_team_func
    
    Aggregate Team Function aggregates scores of 2 Factions (teams) within a match, a component of the pipeline 
    in delivering statistics to the database for collection

    '''

    for faction in match_data.get('items', []):
        faction

    pass
    