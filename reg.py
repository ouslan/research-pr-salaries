from src.data.data_process import DataReg
import arviz as az
import bambi as bmb
import matplotlib.pyplot as plt
import polars as pl

dr = DataReg()

df_qcew = dr.regular_data()

df_qcew.group_by(["year", "qtr", "zipcode", "ein"]).agg(
           total_employment=pl.col("total_employment").sum(),
            total_wages=pl.col("total_wages").sum()
        )
tmp = pl.from_pandas(dr.make_spatial_dataset().drop("geometry", axis=1))

master = df_qcew.join(tmp,on=["year","qtr","zipcode"], how="inner", validate="m:1")

data = master.to_pandas().copy()
data["date2"] = data["year"] * 10 + data["qtr"]
data['date'] = data['date2'].astype('category')
data['zipcode'] = data['zipcode'].astype('category')
data['ein'] = data['ein'].astype('category')


model = bmb.Model(
    "total_employment ~ 0 + k_index + own_children6 + own_children17  + commute_car + food_stamp + with_social_security",
    data, dropna=True,
)
results = model.fit(sample_kwargs={"nuts_sampler" :"blackjax"},cores=15)

az.plot_trace(
    results,
    compact=True,
)

plt.savefig("trace_plot.png", format="png")

res = az.summary(results)
res.to_csv("results.csv")