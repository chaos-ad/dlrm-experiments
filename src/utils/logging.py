import logging
import logging.config
from pathlib import Path
from typing import Union

import yaml

##############################################################################

ROOT_PATH = Path(__file__).parents[2]
CONFIG_PATH_DEFAULT = ROOT_PATH / "conf" / "logging" / "default.yaml"

##############################################################################

def setup(config_file: Union[str, Path] = CONFIG_PATH_DEFAULT) -> None:
    config = yaml.safe_load(Path(config_file).read_text())
    logging.config.dictConfig(config)

##############################################################################
