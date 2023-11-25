import grp
import hashlib
import json
import logging
import os
import pathlib
import shutil
import pwd
from typing import Any, Optional, Sequence, Union

#############################################################################

logger = logging.getLogger(__name__)

#############################################################################


def human_readable_size(size_bytes: int, units: Sequence[str] = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")) -> str:
    """Returns a human readable string representation of bytes"""
    return str(size_bytes) + units[0] if size_bytes < 1024 else human_readable_size(size_bytes >> 10, units[1:])


def get_file_size(file_path: str, human_readable: bool = False) -> Union[int, str]:
    size = pathlib.Path(file_path).stat().st_size
    return human_readable_size(size) if human_readable else size


def get_parent_path(path: str) -> str:
    return str(pathlib.Path(path).parent)

def mkdir(path: str, parents=True, exist_ok=True) -> None:
    logger.info(f"creating path {path}...")
    result = pathlib.Path(path).mkdir(parents=parents, exist_ok=exist_ok)
    logger.info(f"creating path {path}: done")
    return result

def rm(path: str, ignore_errors=True) -> None:
    logger.info(f"removing path '{path}'...")
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path, ignore_errors=ignore_errors)
        logger.info(f"removing path '{path}': done")
    else:
        logger.info(f"removing path '{path}': error (path does not exist)")

def path_exists(path: str) -> bool:
    return pathlib.Path(path).exists()

def deduce_file_format(file_path: str) -> Optional[str]:
    _, file_ext = os.path.splitext(file_path)
    if file_ext:
        file_ext = file_ext[1:]
    return file_ext if file_ext else None

def get_file_format(file_path: str, file_format: Optional[str] = None) -> str:
    file_format = file_format or deduce_file_format(file_path)
    if file_format is None:
        raise ValueError("unknown file format")
    return file_format

def chown(filepath: str, username: str, groupname: str) -> None:
    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam(groupname).gr_gid
    logger.info(f"fixing permissions for '{filepath}' to {uid}:{gid}...")
    os.chown(filepath, uid, gid)
    logger.info(f"fixing permissions for '{filepath}' to {uid}:{gid}: done")


#############################################################################
# Json IO Utils


def save_json(obj: Any, file_path: str) -> None:
    logger.info(f"saving json into '{file_path}'...")
    mkdir(os.path.dirname(file_path))
    file = pathlib.Path(file_path)
    file.write_text(json.dumps(obj))
    size = file.stat().st_size
    logger.info(f"saving json into '{file_path}': done ({human_readable_size(size)})")


def load_json(file_path: str) -> Any:
    logger.info(f"loading json from '{file_path}'...")
    file = pathlib.Path(file_path)
    result = json.loads(file.read_text())
    logger.info(f"loading json from '{file_path}': done")
    return result


#############################################################################


def get_file_hash(path: str, hash_type=hashlib.sha1, blocks=128) -> str:
    hash = hash_type()
    with open(path, 'rb') as f: 
        while chunk := f.read(blocks * hash.block_size): 
            hash.update(chunk)
    return hash.hexdigest()


#############################################################################
