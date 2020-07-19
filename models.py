from pydantic import BaseModel
from typing import Optional


class SimpleMessage(BaseModel):
    message: str
    meta: Optional[str] = None
