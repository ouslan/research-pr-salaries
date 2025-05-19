from src.data.data_process import DataReg
import arviz as az
import bambi as bmb
import matplotlib.pyplot as plt
import polars as pl

dr = DataReg()

df_qcew = dr.regular_data().filter(pl.col("year") >= 2012)
pr_zips = dr.spatial_data()["zipcode"].to_list()

df_qcew = df_qcew.with_columns(
    foreign=pl.when(pl.col("ui_addr_5_zip").is_in(pr_zips)).then(0).otherwise(1)
)

df_qcew = df_qcew.group_by(["year", "qtr", "zipcode", "ein"]).agg(
    total_employment=pl.col("total_employment").sum(),
    total_wages=pl.col("total_wages").sum(),
    foreign=pl.col("foreign").mean(),
)
df_qcew = df_qcew.filter((pl.col("foreign") == 1) | (pl.col("foreign") == 0))

tmp = pl.from_pandas(dr.make_spatial_dataset().drop("geometry", axis=1))

master = df_qcew.join(tmp, on=["year", "qtr", "zipcode"], how="inner", validate="m:1")

master = master.with_columns(k_dummy=pl.col("foreign") * pl.col("k_index"))

data = master.to_pandas().copy()
data["date2"] = data["year"] * 10 + data["qtr"]
data["date"] = data["date2"].astype("category")
data["zipcode"] = data["zipcode"].astype("category")
data["ein"] = data["ein"].astype("category")


# PR buisnesses
data_pr = data[data["foreign"] == 0]
model = bmb.Model(
    "total_employment ~ 0 + k_index + own_children6 + own_children17 + date + ein + commute_car + food_stamp + with_social_security",
    data_pr,
    dropna=True,
)
results = model.fit(sample_kwargs={"nuts_sampler": "blackjax"}, cores=15)

az.plot_trace(results)

plt.savefig("data/results/trace_plot_pr.png", format="png")

res = az.summary(results)
res.to_csv("data/results/results_pr.csv")

# Foreing Buisnesses
data_pr = data[data["foreign"] == 1]
model = bmb.Model(
    "total_employment ~ 0 + k_index + own_children6 + own_children17 + date + ein + commute_car + food_stamp + with_social_security",
    data_pr,
    dropna=True,
)
results = model.fit(sample_kwargs={"nuts_sampler": "blackjax"}, cores=15)

az.plot_trace(results)

plt.savefig("data/results/trace_plot_foreing.png", format="png")

res = az.summary(results)
res.to_csv("data/results/results_foreing.csv")

# Full Model
model = bmb.Model(
    "total_employment ~ 0 + k_index + k_dummy + own_children6 + own_children17 + date + ein + commute_car + food_stamp + with_social_security",
    data,
    dropna=True,
)
results = model.fit(sample_kwargs={"nuts_sampler": "blackjax"}, cores=15)

az.plot_trace(results)

plt.savefig("data/results/trace_plot_full.png", format="png")

res = az.summary(results)
res.to_csv("data/results/results_full.csv")
