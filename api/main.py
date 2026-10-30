from fastapi import FastAPI
from fastapi import APIRouter

from api.utils.dependencies import state
from fastapi.concurrency import run_in_threadpool
from api.routes import predict
from contextlib import asynccontextmanager

import aiohttp
import joblib

import os
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.artifacts = joblib.load('api/artifacts/model.pkl')
    
    connector = aiohttp.TCPConnector(limit = 100, limit_per_host=20)
    timeout = aiohttp.ClientTimeout(20, 5)

    state.session = aiohttp.ClientSession(
        connector= connector,
        timeout= timeout
    )


    yield

    if state.session and not state.session.closed:
        await state.session.close()

app = FastAPI(lifespan= lifespan)
app.include_router(predict.predict_route)


@app.get("/")
async def root():
    return {"message": "Faceit AI Predictor. Win your games powered by AI"}

@app.get("/end")
async def root():
    return {"message": "welcome to the other endpoint."}
