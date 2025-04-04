from src.data.data_process import DataReg
import polars as pl
import arviz as az
import matplotlib.dates as mdates
import geopandas as gpd
import causalpy as cp
import pandas as pd
from shapely import wkt
import matplotlib.pyplot as plt

dr = DataReg()

df = dr.conn.sql(
    "SELECT first_month_employment, second_month_employment, third_month_employment, ui_addr_5_zip, qtr, year FROM qcewtable"
).pl()

pr_zips = gpd.GeoDataFrame(dr.make_spatial_table().df())
pr_zips["geometry"] = pr_zips["geometry"].apply(wkt.loads)
pr_zips = pr_zips.set_geometry("geometry")
pr_zips["zipcode"] = pr_zips["zipcode"].astype(str)
empty_df = [pl.Series("date", [], dtype=pl.String)]
for zips in list(pr_zips["zipcode"].values):
    empty_df.append(pl.Series(f"zip_{zips}", [], dtype=pl.Int32))
df_master = pl.DataFrame(empty_df)

tmp = df.drop_nulls()
tmp = tmp.filter(pl.col("ui_addr_5_zip").is_in(list(pr_zips["zipcode"].values)))
tmp = tmp.group_by(["year", "qtr", "ui_addr_5_zip"]).agg(
    first_month_employment=pl.col("first_month_employment").sum(),
    second_month_employment=pl.col("second_month_employment").sum(),
    third_month_employment=pl.col("third_month_employment").sum(),
)

tmp = tmp.with_columns(ui_addr_5_zip="zip_" + pl.col("ui_addr_5_zip"))


def foo(df: pl.DataFrame, year, qtr):
    df = df.filter((pl.col("year") == year) & (pl.col("qtr") == qtr))
    if df.is_empty():
        return df
    names = df.select(pl.col("ui_addr_5_zip")).transpose()
    names = names.to_dicts().pop()
    df = df.drop("year", "qtr", "ui_addr_5_zip").transpose(include_header=True)
    df = df.rename(names)
    df = df.with_columns(
        date=pl.when((qtr == 1) & (pl.col("column") == "first_month_employment"))
        .then(pl.lit(f"{year}-01-01"))
        .when((qtr == 1) & (pl.col("column") == "second_month_employment"))
        .then(pl.lit(f"{year}-02-01"))
        .when((qtr == 1) & (pl.col("column") == "third_month_employment"))
        .then(pl.lit(f"{year}-03-01"))
        .when((qtr == 2) & (pl.col("column") == "first_month_employment"))
        .then(pl.lit(f"{year}-04-01"))
        .when((qtr == 2) & (pl.col("column") == "second_month_employment"))
        .then(pl.lit(f"{year}-05-01"))
        .when((qtr == 2) & (pl.col("column") == "third_month_employment"))
        .then(pl.lit(f"{year}-06-01"))
        .when((qtr == 3) & (pl.col("column") == "first_month_employment"))
        .then(pl.lit(f"{year}-07-01"))
        .when((qtr == 3) & (pl.col("column") == "second_month_employment"))
        .then(pl.lit(f"{year}-08-01"))
        .when((qtr == 3) & (pl.col("column") == "third_month_employment"))
        .then(pl.lit(f"{year}-09-01"))
        .when((qtr == 4) & (pl.col("column") == "first_month_employment"))
        .then(pl.lit(f"{year}-10-01"))
        .when((qtr == 4) & (pl.col("column") == "second_month_employment"))
        .then(pl.lit(f"{year}-11-01"))
        .when((qtr == 4) & (pl.col("column") == "third_month_employment"))
        .then(pl.lit(f"{year}-12-01"))
        .otherwise(pl.lit("ERROR"))
    )
    return df.drop("column")


for year in range(2002, 2025):
    for qtr in range(1, 5):
        something = foo(tmp, year, qtr)
        if something.is_empty():
            continue
        df_master = pl.concat([df_master, something], how="diagonal")
data = df_master
columns_with_nulls = [col for col in data.columns if data[col].is_null().any()]

data = data.drop(columns_with_nulls)
data = data.to_pandas()
data["date"] = pd.to_datetime(data["date"])
treatment_time = pd.to_datetime("2023-01-01")
data.index = pd.to_datetime(data["date"])
data = data.drop("date", axis=1)


def geolift(df: pd.DataFrame, treatment_time, zipcode: str):
    formula = f"{zipcode} ~ 0"
    for col in data.columns:
        if col == zipcode:
            continue
        formula += f" + {col}"
    result = cp.SyntheticControl(
        df,
        treatment_time,
        formula=formula,
        model=cp.pymc_models.WeightedSumFitter(
            sample_kwargs={
                "target_accept": 0.99,
                "tune": 2000,
                "nuts_sampler": "blackjax",
            }
        ),
    )
    return result


results_df = [
    pl.Series("zipcode", [], dtype=pl.String),
    pl.Series("impact", [], dtype=pl.Float64),
]
results_df = pl.DataFrame(results_df).clear()
for zips in data.columns:
    result = geolift(data, treatment_time, zips)
    fig, ax = result.plot(plot_predictors=False)

    # Formatting
    ax[2].tick_params(axis="x", labelrotation=-90)
    ax[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax[2].xaxis.set_major_locator(mdates.YearLocator())
    for i in [0, 1, 2]:
        ax[i].set(ylabel="Employment")

    # Save the plot
    fig.savefig(f"assets/{zips}.png")

    # Close the figure to release resources
    plt.close(fig)  # This is crucial to avoid memory issues

    index = result.post_impact_cumulative.obs_ind.max()
    last_cumulative_estimate = result.post_impact_cumulative.sel({"obs_ind": index})

    # Summarize the cumulative impact
    summary_stats = az.summary(last_cumulative_estimate, kind="stats")
    temp = pl.DataFrame(
        [
            pl.Series("zipcode", [zips], dtype=pl.String),
            pl.Series("impact", [summary_stats["mean"].max()], dtype=pl.Float64),
        ]
    )

    results_df = pl.concat([results_df, temp], how="vertical")
    del temp, summary_stats, result, index, last_cumulative_estimate

results_df.write_parquet("data/impact.parquet")
