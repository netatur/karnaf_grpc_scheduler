from pydantic import BaseModel


class CallbackRequest(BaseModel):
    id: str
    url_callback: str
    time: int
