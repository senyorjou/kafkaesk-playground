from pydantic import BaseModel
from typing import Optional


class SimpleMessage(BaseModel):
    message: str
    meta: Optional[str] = None


class SimpleTweet(BaseModel):
    message: str
    likes: int = 0
    retweets: int = 0


models = {"SimpleMessage": SimpleMessage, "SimpleTweet": SimpleTweet}
