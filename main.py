from configs.logging_config import setup_logging

setup_logging()

from pipeline.faceitclient import FaceitClient
from pipeline.orch import getdata
from pipeline.runner import PipelineRunner
from dotenv import load_dotenv
import os
import asyncio
import subprocess

load_dotenv()


subprocess.run(
    ['sudo', '-S', 'systemctl', 'start', 'mongod'],
    input=os.environ['p'].encode(),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)

async def main():
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

    await runner.supermatch()

if __name__ == "__main__":
    try:
        asyncio.run(main())

    except SystemExit as e:
        if e.code == 1:
            print("SystemExit(1) occurred — retrying once...")
              
            try:
                asyncio.run(main())
            except SystemExit as e2:
                print(f"Second run exited with code: {e2.code}")
        else:
            raise  # re-raise other exit codes

    except Exception as e:
        print(f"Unexpected error: {e}")
