from pipeline.faceitclient import FaceitClient
from pipeline.orch import getdata
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.environ['API_KEY']

headers = {
    'Authorization': f"Bearer {API_KEY}",
    'Accept': "application/json"
}

host = 'localhost'
port = 27017

fpl_id = '27ccb2d1-2715-4f65-be3f-c5de635fcd31'
falcons_diamond_id = "5259a84f-ceda-448e-a811-f465fe034419"

client = FaceitClient()
players_list = client.retrieve_hub_members(
    hub_id= falcons_diamond_id,
    headers = headers,
)

data = getdata()
data.connect_db(host = host, port = port, connect= True)

data.store_data(batch = players_list,
           collection = 'players')


