import os
import pathlib
from dataclasses import dataclass


@dataclass
class Config:

    dataset_dir: pathlib.Path = pathlib.Path(os.getcwd()) / "datasets"
