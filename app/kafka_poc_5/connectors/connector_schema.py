from pydantic import BaseModel


class ConnectorResponse(BaseModel):
    name: str
    config: dict
