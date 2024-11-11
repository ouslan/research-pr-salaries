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
        self.debug_log("dp03table created")
    
    def semipar_data(self):
        if "dp03table" not in self.conn.list_tables():
            self.make_dp03_dataset()
        if "qcewtable" not in self.conn.list_tables():
            self.make_qcew_dataset()
        df_qcew = self.conn.table("qcewtable")
        df_dp03 = self.conn.table("dp03table").drop("id")

        df_qcew = df_qcew.filter(df_qcew.ein != "", df_qcew.phys_addr_5_zip!= "", df_qcew.year != "", df_qcew.qtr!= "")
        df_qcew = df_qcew.cast({
        "first_month_employment": "float64", "second_month_employment": "float64", "third_month_employment": "float64",
        "year": "int32", "qtr": "int32", "total_wages": "float64"
        })
        df_qcew = df_qcew.mutate(
         total_employment=(df_qcew.first_month_employment + df_qcew.second_month_employment + df_qcew.third_month_employment)/3
        )
        df_qcew = df_qcew.group_by(["year", "qtr", "ein", "phys_addr_5_zip"]).aggregate(
            [df_qcew.total_employment.sum().name("total_employment"), df_qcew.total_wages.sum().name("total_wages")]
            )
        df_qcew = df_qcew.rename(zipcode="phys_addr_5_zip")

        return df_qcew.join(df_dp03, predicates=["year", "qtr", "zipcode"], how="inner")
