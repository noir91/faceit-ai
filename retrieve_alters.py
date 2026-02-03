from ingestion.fetch import alter_function, match
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
player_id = '22531ed3-e6f6-4647-932f-52fd8af17073'

alter_function(
    player_id = player_id ,
    headers = headers,
    host = host,
    port = port
)


