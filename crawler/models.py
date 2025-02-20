from pydantic import BaseModel


class Repository(BaseModel):
    id: str
    name_with_owner: str
    stargazer_count: int
