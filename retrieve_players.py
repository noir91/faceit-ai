from ingestion.fetch import retrieve_hub_members
from pipeline.orch import getdata

API_key = "000e8362-bcf1-4074-bb99-2593f979c8de"

headers = {
    'Authorization': f"Bearer {API_key}",
    'Accept': "application/json"
}

host = 'localhost'
port = 27017

fpl_id = '27ccb2d1-2715-4f65-be3f-c5de635fcd31'
falcons_diamond_id = "5259a84f-ceda-448e-a811-f465fe034419"

players_list = retrieve_hub_members(
    hub_id= falcons_diamond_id,
    headers = headers,
)

data = getdata()
data.connect_db(host = host, port = port, connect= True)

data.store_data(batch = players_list,
           collection = 'players')


