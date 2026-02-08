
from orch import getdata
import time
from faceitclient import FaceitClient

class PipelineRunner():
    """
    Docstring for PipelineRunner

    Stores the Runner and supermatch function, all together automates a strong workflow for
    raw data collection.
    
    - Load context (players, configs, batch size)
    - Orchestrate stages (not implement them)
    - Handle failures + checkpoints
    """

    def __init__(self):
        self.client = FaceitClient()

    def supermatch(self, headers, host, port):
        # Connecting MongoDB 
        data = getdata()
        data.connect_db(host = host, port = port)
        
        # Collect N player_ids
        player_ids = self.collect_N(data = data)

        # accessing player ids for alter func
        for i in range(len(player_ids)):
            success = self.client.alter_function(player_id = player_ids[i],
                        headers = headers,
                        data = data)
            if success == True:
                print(f"({i+1}) Ran player id :{player_ids[i]}, alters and matches stored!")
            else: 
                print(f"({i+1}) Player ID skipped due lesser number of matches in the past 40d!!")
            #time.sleep(0.7)

        # Get statistics of all the existing matches

        # Fetching match_ids query
        matches = data.matches
        match_ids = [id['_id'] for id in matches.find({}, {'_id': 1})]
        players_stats = []

            # I THINK I SHOULD MOVE THIS TO MAIN SCRIPT
        # Storing statistics of each player
        for match_id in match_ids:
            statistics = self.client.statistics_transform(match_id= match_id)
            players_stats.append(statistics)
            data.store_data(batch = players_stats,
                            collection = 'ratings')