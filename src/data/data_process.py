from ..jp_qcew.src.data.data_process import cleanData
from ..models import init_countytable, init_coutsubtable, init_zipstable
import logging
import geopandas as gpd
import ibis
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
        if "countytable" not in self.conn.list_tables():
            self.make_spatial_table()
        df_qcew = self.conn.table("qcewtable")
        # gdf = self.conn.table("countytable")

        df_qcew = df_qcew.filter(~df_qcew.geom.x().isnull())
        df_qcew = df_qcew.filter(df_qcew.geom.x() != 0)
        df_qcew = df_qcew.mutate(
            first_month_employment=df_qcew.first_month_employment.fill_null(0),
            second_month_employment=df_qcew.second_month_employment.fill_null(0),
            third_month_employment=df_qcew.third_month_employment.fill_null(0),
            total_wages=df_qcew.total_wages.fill_null(0),
        )
        # TODO: ensure that there is no null inf or nan in the data
        df_qcew = df_qcew.mutate(
            total_employment=(
                df_qcew.first_month_employment
                + df_qcew.second_month_employment
                + df_qcew.third_month_employment
            )
            / 3
        )

        df_qcew = df_qcew.mutate(
            wages_employee=df_qcew.total_wages / df_qcew.total_employment,
            naics2=df_qcew.naics_code.substr(0, 2),
        )
        df_qcew = df_qcew.mutate(
            wages_employee=ibis.case()
            .when((df_qcew.wages_employee.isnan()), 0.0)
            .else_(df_qcew.wages_employee)
            .end()
        )

        df_qcew = df_qcew.select(
            "id",
            "year",
            "qtr",
            "naics2",
            "phys_addr_5_zip",
            "total_employment",
            "wages_employee",
            "total_wages",
            "geom",
        )
        # NOTE: I could define the first module to prevent master_df to be unbounded
        # for muni in range(1, gdf.id.max().execute() - 1):  # gdf.id.max().execute() - 1
        #     try:
        #         tmp = gdf.filter(gdf.id == muni).geom.as_scalar()
        #         temp = df_qcew.filter(df_qcew.geom.within(tmp))
        #         temp = temp.mutate(county_id=muni)
        #         master_df = ibis.union(master_df, temp)
        #     except NameError:
        #         tmp = gdf.filter(gdf.id == muni).geom.as_scalar()
        #         temp = df_qcew.filter(df_qcew.geom.within(tmp))
        #         temp = temp.mutate(county_id=muni)
        #         master_df = temp

        # df_qcew = df_qcew.group_by(["year", "naics2", "phys_addr_5_zip"]).aggregate(
        #     [
        #         df_qcew.wages_employee.mean().name("mw_industry"),
        #         df_qcew.total_employment.sum().name("total_employment"),
        #     ]
        # )

        # df_qcew = df_qcew.mutate(
        #     min_wage=ibis.case()
        #     .when((df_qcew.year >= 2002) & (df_qcew.year < 2009), 5.15 * 65 * 8)
        #     .when((df_qcew.year >= 2010) & (df_qcew.year < 2023), 7.25 * 65 * 8)
        #     .when((df_qcew.year == 2023), 8.5 * 65 * 8)
        #     .when((df_qcew.year == 2024), 10.5 * 65 * 8)
        #     .else_(None)
        #     .end(),
        # )
        # df_qcew = df_qcew.mutate(k_index=df_qcew.min_wage / df_qcew.mw_industry)

        return df_qcew # df_qcew.join(gdf, df_qcew.county_id == gdf.id)

    def make_spatial_table(self):
        # pull shape files from the census
        if not os.path.exists(f"{self.saving_dir}external/county.zip"):
            self.pull_file(
                url="https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip",
                filename=f"{self.saving_dir}external/county.zip",
            )
            logging.info("Downloaded county shape file")
        if not os.path.exists(f"{self.saving_dir}external/countsub.zip"):
            self.pull_file(
                url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip",
                filename=f"{self.saving_dir}external/countsub.zip",
            )
            logging.info("Downloaded sub county shape file")
        if not os.path.exists(f"{self.saving_dir}external/zips_shape.zip"):
            self.pull_file(
                url="https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip",
                filename=f"{self.saving_dir}external/zips_shape.zip",
            )
            logging.info("Downloaded zipcode shape files")
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
        if "zipstable" not in self.conn.list_tables():
            init_zipstable(self.data_file)
