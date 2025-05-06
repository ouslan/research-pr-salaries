from src.data.data_process import DataReg
import arviz as az
import bambi as bmb
import matplotlib.pyplot as plt
from linearmodels.panel import RandomEffects, PanelOLS


dr = DataReg()

df = dr.make_dataset()

data = df.copy()
data["date2"] = data["year"] * 10 + data["qtr"]
data["date"] = data["date2"].astype("category")
data["zipcode"] = data["zipcode"].astype("category")

data_panel = data.set_index(["zipcode", "date2"])
data_panel = data_panel.drop("geometry", axis=1)
model = RandomEffects.from_formula(
    "total_employment ~ k_index + w_k_index + w_employment + own_children6 + own_children17 + commute_car + food_stamp + with_social_security",
    data=data_panel,
)
results = model.fit(cov_type="clustered", cluster_entity=True)
print(results.summary)

model = bmb.Model(
    "total_employment ~ 0 + k_index + w_k_index + w_employment + own_children6 + own_children17  + commute_car + food_stamp + with_social_security + date + zipcode",
    data,
    dropna=True,
)
results = model.fit(target_accept=0.99, draws=1000, cores=15)

az.plot_trace(
    results,
    compact=True,
)
plt.savefig("data.png")

res = az.summary(results)
res_filtered = res.loc[
    res.index.str.contains("k_index|with_social_security|w_employment")
]
print(res_filtered)
