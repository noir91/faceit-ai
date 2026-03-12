from configs.logging_config import setup_logging

setup_logging()

from pipeline.faceitclient import FaceitClient
from pipeline.orch import getdata
from pipeline.runner import PipelineRunner
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.environ['API_KEY']
host = 'localhost'
port = 27017

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application-json"
}

"""dbobj = getdata()
dbobj.connect_db(host = host,
                 port = port)
client = FaceitClient(
    dbobj= dbobj,
    headers = headers
)
"""
runner = PipelineRunner(
    headers = headers,
    host = host,
    port = port
)

runner.supermatch()



