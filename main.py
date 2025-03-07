from src.data.data_process import DataReg


def main() -> None:
    d = DataReg("data/")
    d.make_qcew_dataset()
    d.make_spatial_table()


if __name__ == "__main__":
    main()
