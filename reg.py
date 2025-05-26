import logging
import jax
import os
import warnings
from pdb import main

import arviz as az
import bambi as bmb
from dotenv import load_dotenv

from src.data.data_process import DataReg
import multiprocessing
import numpyro

os.environ["JAX_PLATFORM_NAME"] = "gpu"
os.environ["XLA_PYTHON_CLIENT_PREALLOCATE"] = "false"
os.environ["XLA_PYTHON_CLIENT_MEM_FRACTION"] = "0.95"

num_cores = multiprocessing.cpu_count()

numpyro.set_platform("gpu")

print("JAX devices:", jax.devices())
print("JAX backend:", jax.default_backend())
print("NumPyro platform:", numpyro.get_platform())
print(f"Available CPU cores: {num_cores}")


def main() -> None:
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    load_dotenv()

    az.style.use("arviz-darkgrid")

    dr = DataReg()
    naics_code = [
        "11",
        "21",
        "22",
        "23",
        "31-33",
        "42",
        "44-45",
        "48-49",
        "51",
        "52",
        "54",
        "55",
        "56",
        "61",
        "62",
        "71",
        "72-accommodation",
        "72-food",
        "81",
        "92",
    ]
    for naics in naics_code:
        data = dr.regular_data(naics=naics)
        for i in ["foreign", "local"]:
            result_path = f"data/processed/results_{i}_{naics}.nc"
            if not os.path.exists(result_path):
                if i == "foreign":
                    data_pr = data[data["foreign"] == 0]
                else:
                    data_pr = data[data["foreign"] == 1]

                model = bmb.Model(
                    "log_total_employment ~ 0 + date + ein + log_k_index + own_children6 + own_children17 + commute_car + food_stamp + with_social_security",
                    data_pr,
                    dropna=True,
                )

                results = model.fit(
                    sample_kwargs={
                        "draws": 500,
                        "tune": 500,
                        "target_accept": 0.8,
                    },
                    cores=15,
                )

                az.to_netcdf(results, result_path)

                dr.notify(
                    url=str(os.environ.get("URL")),
                    auth=str(os.environ.get("AUTH")),
                    msg=f"Successfully completed regression for NAICS {naics} for {i}",
                )
            else:
                logging.info(f"{result_path} already exists")
                continue


if __name__ == "__main__":
    main()
