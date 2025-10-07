import logging
import os
import warnings
from pdb import main

import arviz as az
import bambi as bmb
from dotenv import load_dotenv

from src.data.data_process import DataReg


def main() -> None:
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    load_dotenv()

    dr = DataReg()

    naics_code = ["7223", "7224", "7225"]
    for naics in naics_code:
        for i in ["foreign", "local"]:
            result_path = f"data/processed/results3_{i}_{naics}.nc"
            if not os.path.exists(result_path):
                if i == "foreign":
                    data = dr.regular_data(naics=naics)
                    data_pr = data[data["foreign"] == 1]
                else:
                    data = dr.regular_data(naics=naics)
                    data_pr = data[data["foreign"] == 0]

                print(f"Running {naics} for {i}")

                priors = {
                    "log_k_index": bmb.Prior("Normal", mu=0, sigma=1),
                    "own_children6": bmb.Prior("Normal", mu=0, sigma=1),
                    "own_children17": bmb.Prior("Normal", mu=0, sigma=1),
                    "commute_car": bmb.Prior("Normal", mu=0, sigma=1),
                    "food_stamp": bmb.Prior("Normal", mu=0, sigma=1),
                    "with_social_security": bmb.Prior("Normal", mu=0, sigma=1),
                    "zipcode": bmb.Prior(
                        "Normal", mu=0, sigma=1
                    ),  # fixed effects dummies
                    "date": bmb.Prior("Normal", mu=0, sigma=1),  # fixed effects dummies
                    "sigma": bmb.Prior("Exponential", lam=1),  # error term
                }

                model = bmb.Model(
                    "log_total_employment ~ 0 + zipcode + date + log_k_index + own_children6 + own_children17 + commute_car + food_stamp + with_social_security",
                    data_pr,
                    priors=priors,
                    dropna=True,
                )

                results = model.fit(
                    inference_method="nutpie",
                    sample_kwargs={"target_accept": 0.8},
                    cores=10,
                    chains=10,
                    random_seed=787,
                )

                az.to_netcdf(results, result_path)

                # dr.notify(
                #     url=str(os.environ.get("URL")),
                #     auth=str(os.environ.get("AUTH")),
                #     msg=f"Successfully completed regression for NAICS {naics} for {i}",
                # )
            else:
                print(f"Skipping {naics} for {i}")
                logging.info(f"{result_path} already exists")
                continue


if __name__ == "__main__":
    main()
