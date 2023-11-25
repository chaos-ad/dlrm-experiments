import logging
from pathlib import Path
from typing import Any, Optional, Union

import omegaconf as oc

##############################################################################

ROOT_PATH = Path(__file__).parents[2]
DEFAULT_APP_CONFIG_PATH = ROOT_PATH / "conf" / "app.yaml"

#############################################################################

logger = logging.getLogger(__name__)

#############################################################################

_app_config_singleton: oc.DictConfig

##############################################################################


def setup(config_file: Union[str, Path] = DEFAULT_APP_CONFIG_PATH) -> oc.DictConfig:
    logger.info(f"loading app config '{config_file}'...")
    global _app_config_singleton  # pylint: disable=global-statement

    conf = oc.OmegaConf.load(config_file)
    if isinstance(conf, oc.DictConfig):
        _app_config_singleton = conf
    else:
        raise RuntimeError("config loading failed")
    logger.info(f"loading app config '{config_file}': done")
    return _app_config_singleton


def app() -> oc.DictConfig:
    try:
        return _app_config_singleton
    except NameError:
        return setup()


def get(key: str, default: Any = None, cfg: Optional[oc.DictConfig] = None) -> Any:
    cfg = cfg or app()
    result = oc.OmegaConf.select(
        cfg=cfg, key=key, default=default, throw_on_resolution_failure=True, throw_on_missing=True
    )
    if result is None:
        raise oc.errors.ConfigKeyError(f"Error resolving key '{key}'")
    return result


##############################################################################
