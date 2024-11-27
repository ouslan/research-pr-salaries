from sqlmodel import Field, SQLModel
from geoalchemy2 import Geometry
from sqlalchemy import Column
from typing import Optional

class MuniTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    municipality:  str
    geo_id: str

def create_muni(engine):
    SQLModel.metadata.create_all(engine)