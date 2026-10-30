import aiohttp
import asyncio
from fastapi import Request
class AppState:
    session: aiohttp.ClientSession | None = None

state = AppState()

def get_aiohttp_session() -> aiohttp.ClientSession:
    if state.session is None:
        raise RuntimeError("aiohttp session has not been initialized.")
    return state.session

def get_model_artifacts(request: Request):
    return request.app.state.artifacts