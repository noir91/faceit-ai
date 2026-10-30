from pydantic import BaseModel, StrictInt
from typing import List, LiteralString, Dict

class MatchRequest(BaseModel):
    match_id: str

class PredictResponse(BaseModel):
    message: str
    prediction: StrictInt
    result: Dict