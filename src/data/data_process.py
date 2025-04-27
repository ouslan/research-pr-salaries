import logging
import os
from datetime import datetime

import geopandas as gpd
import pandas as pd
import polars as pl
import requests
from shapely import wkt

from ..jp_qcew.src.data.data_process import cleanData
from ..models import init_dp03_table


class DataReg(cleanData):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def base_data(self) -> pl.DataFrame:
        if "qcewtable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            self.make_qcew_dataset()

        df_qcew = self.conn.sql(
            "SELECT year,qtr,phys_addr_5_zip,first_month_employment,total_wages,second_month_employment,third_month_employment,naics_code FROM qcewtable"
        ).pl()
        df_qcew = df_qcew.rename({"phys_addr_5_zip": "zipcode"})
        df_qcew = df_qcew.filter(
            (pl.col("zipcode") != "") & (pl.col("naics_code") != "")
        )
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
        df_qcew = df_qcew.group_by(["year", "qtr", "zipcode"]).agg(
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

        return df_qcew

    def make_dataset(self):
        df_qcew = self.base_data()
        gdf = self.spatial_data()
        dp03_df = self.pull_dp03()
        dp03_df = dp03_df.with_columns(qtr=4)

        gdf = gdf.join(
            df_qcew.to_pandas().set_index("zipcode"),
            on="zipcode",
            how="inner",
            validate="1:m",
        )

        gdf = gdf.merge(
            dp03_df.to_pandas(),
            on=["year", "qtr", "zipcode"],
            how="left",
            validate="1:1",
        )

        gdf = gdf.sort_values(by=["zipcode", "year", "qtr"]).reset_index(drop=True)
        columns = [
            "inc_25k_35k",
            "inc_35k_50k",
            "inc_50k_75k",
            "inc_75k_100k",
            "inc_100k_150k",
            "inc_150k_200k",
            "inc_more_200k",
        ]
        for col in columns:
            gdf[col] = gdf.groupby("zipcode")[col].transform(
                lambda group: group.interpolate(method="cubic")
            )
        return gdf[(gdf["year"] >= 2012) & (gdf["year"] < 2023)]

    def spatial_data(self) -> gpd.GeoDataFrame:
        gdf = gpd.GeoDataFrame(self.make_spatial_table())
        gdf["geometry"] = gdf["geometry"].apply(wkt.loads)
        gdf = gdf.set_geometry("geometry").set_crs("EPSG:4269", allow_override=True)
        gdf = gdf.to_crs("EPSG:3395")
        gdf["zipcode"] = gdf["zipcode"].astype(str)
        return gdf

    def pull_query(self, params: list, year: int) -> pl.DataFrame:
        # prepare custom census query
        param = ",".join(params)
        base = "https://api.census.gov/data/"
        flow = "/acs/acs5/profile"
        url = f"{base}{year}{flow}?get={param}&for=zip%20code%20tabulation%20area:*&in=state:72"
        df = pl.DataFrame(requests.get(url).json())

        # get names from DataFrame
        names = df.select(pl.col("column_0")).transpose()
        names = names.to_dicts().pop()
        names = dict((k, v.lower()) for k, v in names.items())

        # Pivot table
        df = df.drop("column_0").transpose()
        return df.rename(names).with_columns(year=pl.lit(year))

    def pull_dp03(self) -> pl.DataFrame:
        if "DP03Table" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            init_dp03_table(self.data_file)
        for _year in range(2012, 2023):
            if (
                self.conn.sql(f"SELECT * FROM 'DP03Table' WHERE year={_year}")
                .df()
                .empty
            ):
                try:
                    logging.info(f"pulling {_year} data")
                    tmp = self.pull_query(
                        params=[
                            "DP03_0001E",
                            "DP03_0051E",
                            "DP03_0052E",
                            "DP03_0053E",
                            "DP03_0054E",
                            "DP03_0055E",
                            "DP03_0056E",
                            "DP03_0057E",
                            "DP03_0058E",
                            "DP03_0059E",
                            "DP03_0060E",
                            "DP03_0061E",
                        ],
                        year=_year,
                    )
                    tmp = tmp.rename(
                        {
                            "dp03_0001e": "total_population",
                            "dp03_0051e": "total_house",
                            "dp03_0052e": "inc_less_10k",
                            "dp03_0053e": "inc_10k_15k",
                            "dp03_0054e": "inc_15k_25k",
                            "dp03_0055e": "inc_25k_35k",
                            "dp03_0056e": "inc_35k_50k",
                            "dp03_0057e": "inc_50k_75k",
                            "dp03_0058e": "inc_75k_100k",
                            "dp03_0059e": "inc_100k_150k",
                            "dp03_0060e": "inc_150k_200k",
                            "dp03_0061e": "inc_more_200k",
                        }
                    )
                    tmp = tmp.rename({"zip code tabulation area": "zipcode"}).drop(
                        ["state"]
                    )
                    tmp = tmp.with_columns(pl.all().exclude("zipcode").cast(pl.Int64))
                    self.conn.sql("INSERT INTO 'DP03Table' BY NAME SELECT * FROM tmp")
                    logging.info(f"succesfully inserting {_year}")
                except:
                    logging.warning(f"The ACS for {_year} is not availabe")
                    continue
            else:
                logging.info(f"data for {_year} is in the database")
                continue
        return self.conn.sql("SELECT * FROM 'DP03Table';").pl()

    def make_spatial_table(self) -> pd.DataFrame:
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
            gdf = gdf.rename(columns={"ZCTA5CE20": "zipcode"}).reset_index()
            gdf = gdf[["zipcode", "geometry"]]
            gdf["zipcode"] = gdf["zipcode"].str.strip()
            df = gdf.drop(columns="geometry")
            geometry = gdf["geometry"].apply(lambda geom: geom.wkt)
            df["geometry"] = geometry
            self.conn.execute("CREATE TABLE zipstable AS SELECT * FROM df")
            logging.info(
                f"The zipstable is empty inserting {self.saving_dir}external/cousub.zip"
            )
        return self.conn.sql("SELECT * FROM zipstable;").df()
