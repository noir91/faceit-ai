from fastapi import APIRouter
import os 

from pathlib import Path
import aiohttp

from api.clients import faceit
from pipeline.faceitclient import FaceitClient

from dotenv import load_dotenv

from api.schemas.prediction import MatchRequest, PredictResponse
from api.utils.dependencies import get_aiohttp_session, get_model_artifacts
from fastapi import Depends

import asyncio

from api.features.builder import build_match_row
import polars as pl



path = Path(__file__).resolve().parents[2]
load_dotenv(f'{path}/.env')

API_KEY = os.environ['API_KEY']
BASE_URL = "https://open.faceit.com/data/v4/"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application-json"
}

predict_route = APIRouter(
    prefix = '/predict')

# httpx_client = httpx.AsyncClient(
#                 base_url= BASE_URL,
#                 headers= HEADERS ,
#                 limits= httpx.Limits(
#                     max_connections = 10,
#                     max_keepalive_connections = 5,
#                 ),
#                 timeout = httpx.Timeout(20.0, pool = 5.0)
#             )

@predict_route.post('/', response_model = PredictResponse)
async def predict(payload: MatchRequest,
                  session: aiohttp.ClientSession = Depends(get_aiohttp_session),
                  artifacts = Depends(get_model_artifacts)):
    faceitclient = FaceitClient(None, HEADERS)
    recent_semaphore = asyncio.Semaphore(4)
    


    result = await faceit.fetch_match_inputs(
            match_id = payload.match_id,
            client=faceitclient,
            session=session,
            recent_semaphore=recent_semaphore,
        )

    # Transform the response into one row
    row = build_match_row(result)

    # Convert to a single-row DataFrame
    df = pl.DataFrame([row])

    # Match the Float32 types used for player features in training
    player_cols = [c for c in df.columns if c.startswith(tuple(f"p{i}_" for i in range(1, 11)))]
    df = df.with_columns(pl.col(player_cols).cast(pl.Float32))

    # Select the exact feature columns/order used to train your model
    model_feature_columns = artifacts['feature_columns']
    X = df.select(model_feature_columns)

    prediction = artifacts['model'].predict(X)[0].item()

    return {"message": "Team 2 wins!" if prediction == 0  else "Team 1 wins!",
            "prediction": prediction, "result": result }



