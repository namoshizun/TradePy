import os
import pathlib
from dataclasses import dataclass


@dataclass
class Config:
    database_dir: pathlib.Path = pathlib.Path(os.getcwd()) / "database"
