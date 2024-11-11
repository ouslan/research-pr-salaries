from src.data.data_process import DataReg

def main() -> None:
    d = DataReg()
    d.make_dp03_dataset()
    d.make_qcew_dataset()

if __name__ == "__main__":
    main()