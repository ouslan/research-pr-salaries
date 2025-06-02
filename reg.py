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

    naics_code2 = [
        "72-food",
        "23",
        "44-45",
        "52",
        "54",
        "56",
        "62",
        "81",
    ]
    naics_code = [
        "72-food",
        "23",
        "44-45",
        "52",
        "54",
        "56",
        "62",
        "81",
        "11",
        "21",
        "22",
        "31-33",
        "42",
        "48-49",
        "51",
        "55",
        "61",
        "71",
        "72-accommodation",
        "72-food",
        "92",
    ]
    for naics in naics_code:
        for i in ["foreign", "local"]:
            result_path = f"data/processed/results_{i}_{naics}.nc"
            if not os.path.exists(result_path):
                data = dr.regular_data(naics=naics)
                if i == "foreign":
                    data_pr = data[data["foreign"] == 1]
                else:
                    data_pr = data[data["foreign"] == 0]

                print(f"Running {naics} for {i}")
                model = bmb.Model(
                    "log_total_employment ~ 0 + ein + date + log_k_index + own_children6 + own_children17 + commute_car + food_stamp + with_social_security",
                    data_pr,
                    dropna=True,
                )

                results = model.fit(
                    inference_method="nutpie",
                    sample_kwargs={"draws": 500, "tune": 500, "target_accept": 0.8},
                    cores=10,
                    chains=10,
                    random_seed=787,
                )

                az.to_netcdf(results, result_path)

                dr.notify(
                    url=str(os.environ.get("URL")),
                    auth=str(os.environ.get("AUTH")),
                    msg=f"Successfully completed regression for NAICS {naics} for {i}",
                )
            else:
                print(f"Skipping {naics} for {i}")
                logging.info(f"{result_path} already exists")
                continue

    for naics in naics_code2:
        data = dr.regular_data(naics=naics)
        for i in ["foreign", "local"]:
            result_path = f"data/processed/results_{i}_{naics}.nc"
            if not os.path.exists(result_path):
                if i == "foreign":
                    data_pr = data[data["foreign"] == 1]
                else:
                    data_pr = data[data["foreign"] == 0]

                print(f"Running {naics} for {i}")
                all_traces = []

                model = bmb.Model(
                    "log_total_employment ~ (1|ein) + (1|date)+ log_k_index + own_children6 + own_children17 + commute_car + food_stamp + with_social_security",
                    data_pr,
                    dropna=True,
                )

                for batch in range(5):  # 5 batches of 2 chains
                    trace = model.fit(
                        inference_method="nutpie",
                        sample_kwargs={"draws": 500, "tune": 500, "target_accept": 0.8},
                        chains=2,
                        cores=1,
                        random_seed=787 + batch,
                    )
                    all_traces.append(trace)

                results = az.concat(all_traces, dim="chain")
                az.to_netcdf(results, result_path)

                dr.notify(
                    url=str(os.environ.get("URL")),
                    auth=str(os.environ.get("AUTH")),
                    msg=f"Successfully completed regression for NAICS {naics} for {i}",
                )
            else:
                print(f"Skipping {naics} for {i}")
                logging.info(f"{result_path} already exists")
                continue


if __name__ == "__main__":
    main()
