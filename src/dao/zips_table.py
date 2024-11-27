from sqlalchemy import create_engine, text
from sqlmodel import SQLModel, Field, Column
from typing import Optional, Any

# Define the ZipsTable model with a MULTIPOLYGON geometry field
class ZipsTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    zipcode: str
    geo_id: str
    

# Function to create the table and enable PostGIS extension
def create_zips(engine):
    SQLModel.metadata.create_all(engine)
