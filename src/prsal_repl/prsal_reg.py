from .utils import DataUtils


class PrsalReg(DataUtils):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
        seed: int = 787,
    ):
        super().__init__(saving_dir, database_file, log_file)
