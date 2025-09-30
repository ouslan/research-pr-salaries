from src.data.data_process import DataReg
import arviz as az
import bambi as bmb
import matplotlib.pyplot as plt
from linearmodels.panel import RandomEffects, PanelOLS
import polars as pl


dr = DataReg()

df_qcew = dr.regular_data()

print(
    df_qcew.group_by(["year", "qtr", "ein"]).agg(
            total_employment=pl.col("total_employment").sum(),
        )
)
