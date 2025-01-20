from ..dao.dp03_table import create_dp03
from ..jp_qcew.src.data.data_process import cleanData
from ..dao.zips_table import create_zips
from ..dao.muni_table import create_muni
import pandas as pd
import geopandas as gpd
import os

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
        if "munitable" not in self.conn.list_tables() or "zipstable" not in self.conn.list_tables():
            self.make_spatial_table()

        df_qcew = self.conn.table("qcewtable")
        df_dp03 = self.conn.table("dp03table").drop("id")

        df_qcew = df_qcew.filter(df_qcew.ein != "", df_qcew.phys_addr_5_zip!= "", df_qcew.year != "", df_qcew.qtr!= "")
        df_qcew = df_qcew.cast({
        "first_month_employment": "float64", "second_month_employment": "float64", "third_month_employment": "float64",
        "year": "int32", "qtr": "int32", "total_wages": "float64", "latitude":"float64", "longitude":"float64"
        })
        df_qcew = df_qcew.mutate(
         total_employment=(df_qcew.first_month_employment + df_qcew.second_month_employment + df_qcew.third_month_employment)/3
        )
        # df_qcew = df_qcew.group_by(["year", "qtr", "ein", "phys_addr_5_zip"]).aggregate(
        #     [df_qcew.total_employment.sum().name("total_employment"), df_qcew.total_wages.sum().name("total_wages")]
        #     )
        df_qcew = df_qcew.rename(zipcode="phys_addr_5_zip")

        return df_qcew#.join(df_dp03, predicates=["year", "qtr", "zipcode"], how="left")


    def make_spatial_table(self):

        create_zips(self.engine)
        create_muni(self.engine)

        if not os.path.exists(f"{self.saving_dir}external/zip_shape.zip"):
            self.pull_file(url="https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip", filename=f"{self.saving_dir}external/zip_shape.zip")
            self.debug_log("zip_shape.zip downloaded")
        if not os.path.exists(f"{self.saving_dir}external/county.zip"):
            self.pull_file(url="https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip", filename=f"{self.saving_dir}external/county.zip")
            self.debug_log("county.zip downloaded")
        zips = gpd.read_file(f"{self.saving_dir}external/zip_shape.zip")
        zips = zips[zips["ZCTA5CE20"].str.startswith("00")]

        gdf = gpd.read_file(f"{self.saving_dir}external/county.zip")
        gdf = gdf[gdf["STATEFP"].str.startswith("72")].rename(columns={"geometry":"count_geo"})

        master_df = gpd.GeoDataFrame(columns=["ZCTA5CE20", "GEOID"])
        for geo in gdf["GEOID"].values:
            tmp = gdf[gdf["GEOID"] == geo]
            tmp = zips.clip(tmp['count_geo'])
            tmp["GEOID"] = geo
            master_df = pd.concat([master_df, tmp[["ZCTA5CE20", "GEOID"]]], ignore_index=True)
        master_df = pd.merge(zips, master_df.drop_duplicates(subset=['ZCTA5CE20']),on="ZCTA5CE20", validate="1:1") 
        master_df = pd.merge(master_df, gdf, on="GEOID", validate="m:1") 
        master_df = master_df[["GEOID", "ZCTA5CE20"]].rename(columns={"GEOID":"geo_id", "ZCTA5CE20":"zipcode"})
        gdf = gdf[["GEOID", "NAME"]].rename(columns={"GEOID":"geo_id", "NAME":"municipality"}).reset_index(drop=True)

        self.conn.insert("zipstable", master_df)
        self.debug_log("zipstable created")
        self.conn.insert("munitable", gdf)
        self.debug_log("munitable created")
