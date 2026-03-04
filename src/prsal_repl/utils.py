import logging
import os

import geopandas as gpd
import pandas as pd
import polars as pl
import tempfile
import requests
from libpysal import weights
from jp_qcew import CleanQCEW
from pathlib import Path
from CensusForge import CensusAPI
from jp_tools import download


class DataUtils(CleanQCEW):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)
        # INFO: This will be fixed in future versions so that jp_qcew dosent return a DF
        # self.make_qcew_dataset()

    def base_spatial_data(self) -> pl.DataFrame:
        df_qcew = self.base_data()

        df_qcew = df_qcew.group_by(["year", "qtr", "zipcode"]).agg(
            mw_industry=pl.col("wages_employee").mean(),
            zip_employment=pl.col("total_employment").mean(),
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
        df_qcew = df_qcew.filter(pl.col("zipcode") != "00611")
        df_qcew = df_qcew.filter(pl.col("zipcode") != "00636")
        df_qcew = df_qcew.filter(pl.col("zipcode") != "20121")
        df_qcew = df_qcew.filter(pl.col("zipcode") != "20123")

        return df_qcew

    def regular_data(self, naics: str):
        df_qcew = self.base_data()  # .filter(pl.col("year") >= 2012)
        if naics == "31-33":
            df_qcew = df_qcew.filter(
                (pl.col("naics_code").str.starts_with("31"))
                | (pl.col("naics_code").str.starts_with("32"))
                | (pl.col("naics_code").str.starts_with("33"))
            )
        elif naics == "44-45":
            df_qcew = df_qcew.filter(
                (pl.col("naics_code").str.starts_with("44"))
                | (pl.col("naics_code").str.starts_with("45"))
            )
        elif naics == "48-49":
            df_qcew = df_qcew.filter(
                (pl.col("naics_code").str.starts_with("48"))
                | (pl.col("naics_code").str.starts_with("49"))
            )
        elif naics == "72-accommodation":
            df_qcew = df_qcew.filter(
                (pl.col("naics_code").str.starts_with("7211"))
                | (pl.col("naics_code").str.starts_with("7212"))
                | (pl.col("naics_code").str.starts_with("7213"))
            )
        elif naics == "72-food":
            df_qcew = df_qcew.filter(
                (pl.col("naics_code").str.starts_with("7223"))
                | (pl.col("naics_code").str.starts_with("7224"))
                | (pl.col("naics_code").str.starts_with("7225"))
            )
        else:
            df_qcew = df_qcew.filter(pl.col("naics_code").str.starts_with(naics))

        df_qcew = df_qcew.filter(pl.col("ein") != "")
        pr_zips = self.spatial_data()["zipcode"].to_list()

        df_qcew = df_qcew.with_columns(
            foreign=pl.when(pl.col("ui_addr_5_zip").is_in(pr_zips)).then(0).otherwise(1)
        )

        df_qcew = df_qcew.group_by(["year", "qtr", "zipcode"]).agg(
            total_employment=pl.col("total_employment").sum(),
            total_wages=pl.col("total_wages").sum(),
            foreign=pl.col("foreign").mean(),
        )
        df_qcew = df_qcew.filter((pl.col("foreign") == 1) | (pl.col("foreign") == 0))

        # df_qcew = df_qcew.filter(pl.col("year")>= 2020)

        tmp = pl.from_pandas(self.make_spatial_dataset().drop("geometry", axis=1))

        master = df_qcew.join(
            tmp, on=["year", "qtr", "zipcode"], how="inner", validate="m:1"
        )

        # List of columns that we want to exclude
        exclude_columns = ["year", "zipcode", "qtr", "state"]

        # Get all columns except the excluded ones
        columns_to_transform = [
            col for col in master.columns if col not in exclude_columns
        ]

        # Create a new list of expressions to add log-transformed columns
        log_columns = [
            (pl.col(col).log().alias(f"log_{col}")) for col in columns_to_transform
        ]

        # Add the log-transformed columns to the DataFrame
        master = master.with_columns(log_columns)
        master = master.with_columns(
            treated=pl.when(pl.col("foreign") == 1).then(True).otherwise(False)
        )
        master = master.with_columns(k_dummy=pl.col("foreign") * pl.col("log_k_index"))

        data = master.to_pandas().copy()

        data["date2"] = data["year"] * 10 + data["qtr"]
        data["date"] = data["date2"].astype("category")
        data["zipcode"] = data["zipcode"].astype("category")
        data = data.sort_values(["year", "qtr", "zipcode"]).reset_index(drop=True)

        return data

    def base_data(self) -> pl.DataFrame:

        df_qcew = self.conn.execute(f"""
            SELECT
                year,
                qtr,
                phys_addr_5_zip,
                ui_addr_5_zip,
                mail_addr_5_zip,
                ein,
                first_month_employment,
                total_wages,
                second_month_employment,
                third_month_employment,
                naics_code
            FROM '{self.saving_dir}processed/pr-qcew-*.parquet';
            """).pl()

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
        return df_qcew

    def make_pr_dataset(self):
        df_qcew = self.base_spatial_data()
        gdf = self.zips_goem()
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
            "total_population",
            "own_children6",
            "own_children17",
            "commute_car",
            "total_house",
            "with_social_security",
            "food_stamp",
        ]
        for col in columns:
            gdf[col] = gdf.groupby("zipcode")[col].transform(
                lambda group: group.interpolate(method="cubic")
            )

        gdf = gdf[(gdf["year"] >= 2012) & (gdf["year"] < 2024)]

        base = gdf[(gdf["year"] == 2012) & (gdf["qtr"] == 4)].copy()
        base = base.sort_values("zipcode")

        # Project for distance calculations
        base_proj = base.to_crs(epsg=5070)

        w_queen = weights.Queen.from_dataframe(base)
        w_queen.transform = "r"

        w_knn6 = weights.KNN.from_dataframe(base_proj, k=6)
        w_knn6.transform = "r"

        w_dist30 = weights.DistanceBand.from_dataframe(
            base_proj, threshold=48280, binary=True, silence_warnings=True
        )

        w_dist30.transform = "r"

        zip_order = base["zipcode"].values

        spatial_lag_results = []

        for year in range(2012, 2024):
            for qtr in range(1, 5):

                group_df = gdf[(gdf["year"] == year) & (gdf["qtr"] == qtr)].copy()

                group_df = group_df.set_index("zipcode").loc[zip_order].reset_index()

                y_vals = group_df["zip_employment"].values
                x_vals = group_df["k_index"].values

                # Queen
                group_df["wq_employment"] = weights.lag_spatial(w_queen, y_vals)
                group_df["wq_k_index"] = weights.lag_spatial(w_queen, x_vals)

                # KNN6
                group_df["wk6_employment"] = weights.lag_spatial(w_knn6, y_vals)
                group_df["wk6_k_index"] = weights.lag_spatial(w_knn6, x_vals)

                # Distance 30 miles
                group_df["wd30_employment"] = weights.lag_spatial(w_dist30, y_vals)
                group_df["wd30_k_index"] = weights.lag_spatial(w_dist30, x_vals)

                spatial_lag_results.append(group_df)

        gdf = pd.concat(spatial_lag_results)

        # Concatenate all the results back together
        gdf = pd.concat(spatial_lag_results)
        gdf = gdf[(gdf["year"] >= 2012) & (gdf["year"] < 2023)]
        return gdf

    def make_spatial_dataset(self):
        df_qcew = self.base_spatial_data()
        gdf = self.zips_goem()
        gdf = gdf.join(
            df_qcew.to_pandas().set_index("zipcode"),
            on="zipcode",
            how="inner",
            validate="1:m",
        )

        gdf = gdf.sort_values(by=["zipcode", "year", "qtr"]).reset_index(drop=True)
        return gdf

    def pull_dp03(self) -> pl.DataFrame:
        for _year in range(2011, 2024):
            file_path = Path(f"{self.saving_dir}raw/acs5-{_year}.parquet")
            if not file_path.exists():

                logging.info(f"pulling {_year} data")
                r = CensusAPI().query(
                    params_list=[
                        "DP03_0001E",
                        "DP03_0008E",
                        "DP03_0009E",
                        "DP03_0014E",
                        "DP03_0016E",
                        "DP03_0019E",
                        "DP03_0025E",
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
                        "DP03_0070E",
                        "DP03_0074E",
                    ],
                    year=_year,
                    geography="zip code tabulation area",
                    dataset="acs-acs5-profile",
                )
                df = pl.DataFrame(r)
                df = df.rename(df.row(0, named=True))
                df = df.slice(1).with_columns(
                    pl.col("*").exclude("zip code tabulation area").cast(pl.Float32)
                )
                df = df.rename(
                    {
                        "DP03_0001E": "total_population",
                        "DP03_0008E": "in_labor_force",
                        "DP03_0009E": "unemployment",
                        "DP03_0014E": "own_children6",
                        "DP03_0016E": "own_children17",
                        "DP03_0019E": "commute_car",
                        "DP03_0025E": "commute_time",
                        "DP03_0051E": "total_house",
                        "DP03_0052E": "inc_less_10k",
                        "DP03_0053E": "inc_10k_15k",
                        "DP03_0054E": "inc_15k_25k",
                        "DP03_0055E": "inc_25k_35k",
                        "DP03_0056E": "inc_35k_50k",
                        "DP03_0057E": "inc_50k_75k",
                        "DP03_0058E": "inc_75k_100k",
                        "DP03_0059E": "inc_100k_150k",
                        "DP03_0060E": "inc_150k_200k",
                        "DP03_0061E": "inc_more_200k",
                        "DP03_0070E": "with_social_security",
                        "DP03_0074E": "food_stamp",
                    }
                )
                df = df.rename({"zip code tabulation area": "zipcode"})
                df = df.with_columns(year=_year)
                df = df.select(pl.col("*").exclude("state"))
                df.write_parquet(file=file_path)
                logging.info(f"succesfully inserting {_year}")
        return self.conn.sql(
            f"SELECT * FROM '{self.saving_dir}raw/acs5-*.parquet';"
        ).pl()

    def zips_goem(self) -> pd.DataFrame:
        file_path = Path(f"{self.saving_dir}external/geo-zips.parquet")
        if not file_path.exists():
            download(
                url="https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip",
                filename=f"{tempfile.gettempdir()}/{hash(file_path)}.zip",
            )
            # Process files
            gdf = gpd.read_file(f"{tempfile.gettempdir()}/{hash(file_path)}.zip")
            gdf = gdf.rename(columns={"ZCTA5CE20": "zipcode"})
            gdf = gdf[gdf["zipcode"].str.startswith("00")].reset_index(drop=True)
            gdf = gdf[["zipcode", "geometry"]]
            gdf["zipcode"] = gdf["zipcode"].str.strip()

            # Remove disjointed goemetries
            gdf = gdf[~gdf["zipcode"].isin(["00820", "00850", "00851"])]
            gdf = gdf.to_crs(epsg=5070)
            gdf = gdf.explode(ignore_index=True)
            connected_mask = gdf.geometry.apply(
                lambda geom: gdf.geometry.intersects(geom).sum() > 1
            )
            gdf = gdf.loc[connected_mask]
            gdf = gdf.dissolve(by="zipcode", as_index=False)
            gdf.to_parquet(file_path)
            logging.info(
                f"The zipstable is empty inserting {self.saving_dir}external/cousub.zip"
            )
        return gpd.read_parquet(path=file_path)

    def calculate_spatial_lag(self, df, w, column):
        # Reshape y to match the number of rows in the dataframe
        y = df[column].values.reshape(-1, 1)

        # Apply spatial lag
        spatial_lag = weights.lag_spatial(w, y)

        return spatial_lag

    def notify(self, url: str, auth: str, msg: str):
        requests.post(
            url,
            data=msg,
            headers={"Authorization": auth},
        )
