from ..dao.dp03_table import create_dp03
from ..jp_qcew.src.data.data_process import cleanData
import pandas as pd
import polars as pl

class DataReg(cleanData):
    
    def __init__(self, saving_dir:str='data/', database_url:str="sqlite:///db.sqlite", debug:bool=True):
        super().__init__(saving_dir, database_url, debug)

    def make_dp03_dataset(self):
        if "dp03table" not in self.conn.list_tables():
            create_dp03(self.engine)
        df = pd.read_stata(f"{self.saving_dir}external/zipcode.dta")
        df["zipcode"] = df["zipcode"].astype("str").str.zfill(5)
        self.conn.insert("dp03table", df)