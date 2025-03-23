from ..jp_qcew.src.data.data_process import cleanData
from ..models import init_countytable, init_coutsubtable, init_zipstable
import logging
import geopandas as gpd
import polars as pl
import os


class DataReg(cleanData):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def base_data(self):
        if "qcewtable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            self.make_qcew_dataset()

        df_qcew = self.conn.sql(
            "SELECT year,qtr,phys_addr_5_zip,first_month_employment,total_wages,second_month_employment,third_month_employment,naics_code FROM qcewtable"
        ).pl()  # TODO: ensure that there is no null inf or nan in the data
        df_qcew = df_qcew.rename({"phys_addr_5_zip":"zipcode"})
        df_qcew = df_qcew.filter((pl.col("zipcode") != "") & (pl.col("naics_code") != ""))
        df_qcew = df_qcew.with_columns(
            first_month_employment=pl.col("first_month_employment").fill_null(
                strategy="zero"
            ),
            second_month_employment=pl.col("second_month_employment").fill_null(
                strategy="zero"
            ),
            third_month_employment=pl.col("third_month_employment").fill_null(
                strategy="zero"
            ),
            total_wages=pl.col("total_wages").fill_null(strategy="zero"),
        )
        df_qcew = df_qcew.with_columns(
            total_employment=(
                pl.col("first_month_employment")
                + pl.col("second_month_employment")
                + pl.col("third_month_employment")
            )
            / 3
        )
        df_qcew = df_qcew.filter(
            (pl.col("total_employment") != 0) & (pl.col("total_wages") != 0)
        )
        df_qcew = df_qcew.with_columns(
            wages_employee=pl.col("total_wages") / pl.col("total_employment"),
            sector=pl.col("naics_code").str.slice(0, 2),
        )
        df_qcew = df_qcew.group_by(["year", "sector", "zipcode"]).agg(
            mw_industry=pl.col("wages_employee").mean(),
            total_employment=pl.col("total_employment").mean(),
        )
        df_qcew = df_qcew.with_columns(
            min_wage=pl.when((pl.col("year") >= 2002) & (pl.col("year") < 2010))
            .then(5.15 * 65 * 8)
            .when((pl.col("year") >= 2010) & (pl.col("year") < 2023))
            .then(7.25 * 65 * 8)
            .when(pl.col("year") == 2023)
            .then(8.5 * 65 * 8)
            .when(pl.col("year") == 2024)
            .then(10.5 * 65 * 8)
            .otherwise(-1)
        )
        df_qcew = df_qcew.with_columns(
            k_index=pl.col("min_wage") / pl.col("mw_industry")
        )

        return df_qcew  # df_qcew.join(gdf, df_qcew.county_id == gdf.id)

    def make_spatial_table(self):
        # initiiate the database tables
        if "zipstable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            # Download the shape files
            if not os.path.exists(f"{self.saving_dir}external/zips_shape.zip"):
                self.pull_file(
                    url="https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip",
                    filename=f"{self.saving_dir}external/zips_shape.zip",
                )
                logging.info("Downloaded zipcode shape files")

            # Process and insert the shape files
            gdf = gpd.read_file(f"{self.saving_dir}external/zips_shape.zip")
            gdf = gdf[gdf["ZCTA5CE20"].str.startswith("00")]
            gdf = gdf.rename({"ZCTA5CE20": "zipcode"}).reset_index()
            gdf = gdf[["zipcode", "geometry"]]
            gdf["zipcode"] = gdf["zipcode"].str.strip()
            df = gdf.drop(columns="geometry")
            geometry = gdf["geometry"].apply(lambda geom: geom.wkt)
            df["geometry"] = geometry
            self.conn.execute("CREATE TABLE zipstable AS SELECT * FROM df")
            logging.info(
                f"The zipstable is empty inserting {self.saving_dir}external/cousub.zip"
            )
        return self.conn.sql("SELECT * FROM zipstable;")
