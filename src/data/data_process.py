from ..jp_qcew.src.data.data_process import cleanData
from ..models import *
import pandas as pd
import geopandas as gpd
import os


class DataReg(cleanData):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_url: str = "duckdb:///data.ddb",
    ):
        super().__init__(saving_dir, database_url)

    def semipar_data(self):
        if "qcewtable" not in self.conn.list_tables():
            self.make_qcew_dataset()
        df_qcew = self.conn.table("qcewtable")

        df_qcew = df_qcew.mutate(
            total_employment=(
                df_qcew.first_month_employment
                + df_qcew.second_month_employment
                + df_qcew.third_month_employment
            )
            / 3
        )
        df_qcew = df_qcew.filter(~df_qcew.geom.x().isnull())
        df_qcew = df_qcew.filter(df_qcew.geom.x() != 0)

        return df_qcew.select(
            "year", "naics_code", "total_employment", "total_wages", "geom"
        )

    def make_spatial_table(self):
        # pull shape files from the census
        if not os.path.exists(f"{self.saving_dir}external/county.zip"):
            self.pull_file(
                url="https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip",
                filename=f"{self.saving_dir}external/county.zip",
            )
        if not os.path.exists(f"{self.saving_dir}external/countsub.shp"):
            self.pull_file(
                url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip",
                filename=f"{self.saving_dir}external/countsub.zip",
            )

        # initiiate the database tables
        if "countytable" not in self.conn.list_tables():
            init_countytable(self.data_file)
            gdf = gpd.read_file("data/external/county.zip", engine="pyogrio")
            gdf = gdf[gdf["STATEFP"] == "72"]
            gdf = gdf[["GEOID", "NAME", "geometry"]].reset_index(drop=True)
            gdf = gdf.rename(
                columns={"GEOID": "geo_id", "NAME": "name", "geometry": "geom"}
            )
            gdf["geom"] = gdf["geom"].apply(lambda x: x.wkt)
            self.conn.insert("countytable", gdf)
        if "countsubtable" not in self.conn.list_tables():
            init_coutsubtable(self.data_file)
