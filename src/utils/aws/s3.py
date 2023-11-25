import logging
import os
from typing import Tuple

import boto3
import utils.filesystem

#############################################################################

logger = logging.getLogger(__name__)

#############################################################################

S3_PREFIX = "s3://"
S3_PREFIX_LEN = len(S3_PREFIX)

#############################################################################


def is_s3_path(path: str) -> bool:
    return path.startswith(S3_PREFIX)


def validate_s3_path(path: str) -> str:
    if is_s3_path(path):
        return path
    raise ValueError("Invalid S3 URI used as an argument")


def truncate_s3_prefix(s3_path: str, validate: bool = True) -> str:
    if validate:
        validate_s3_path(s3_path)
    return s3_path[S3_PREFIX_LEN:]


def truncate_trailing_slashes(path: str) -> str:
    while path.startswith("/"):
        path = path[1:]
    return path


#############################################################################


def get_bucket(s3_path: str) -> str:
    s3_path_trunc = truncate_s3_prefix(s3_path)
    return s3_path_trunc.split("/")[0]


def get_postfix(s3_path: str) -> str:
    s3_bucket = get_bucket(s3_path)
    s3_postfix = s3_path[5 + len(s3_bucket) :]
    return s3_postfix


def parse_path(s3_path: str) -> Tuple[str, str]:
    s3_bucket = get_bucket(s3_path)
    s3_postfix = s3_path[S3_PREFIX_LEN + len(s3_bucket) + 1 :]
    return s3_bucket, s3_postfix


#############################################################################


def upload_file(local_path: str, s3_path: str) -> None:
    logger.info(f"uploading file '{local_path}' -> '{s3_path}'...")
    s3_bucket, s3_postfix = parse_path(s3_path)
    bucket = boto3.resource("s3").Bucket(s3_bucket)
    bucket.upload_file(local_path, s3_postfix)
    logger.info(f"uploading file '{local_path}' -> '{s3_path}': done")


def upload_folder(local_path: str, s3_path: str) -> None:
    logger.info(f"uploading folder '{local_path}' -> '{s3_path}'...")

    file_count = 0
    for local_subdir, _, local_files in os.walk(local_path):
        for local_file in local_files:
            local_postfix = truncate_trailing_slashes(local_subdir[len(local_path) :])
            src_file_path = os.path.join(local_path, local_postfix, local_file)
            dst_file_path = os.path.join(s3_path, local_postfix, local_file)
            upload_file(src_file_path, dst_file_path)
            file_count += 1

    if not file_count:
        logger.info(f"uploading folder '{local_path}' -> '{s3_path}': done (nothing to copy)")
    else:
        logger.info(f"uploading folder '{local_path}' -> '{s3_path}': done ({file_count} files)")


#############################################################################


def download_file(s3_path: str, local_path: str) -> None:
    logger.info(f"downloading file '{s3_path}' -> '{local_path}'...")
    s3_bucket, s3_postfix = parse_path(s3_path)
    bucket = boto3.resource("s3").Bucket(s3_bucket)
    utils.filesystem.mkdir(os.path.dirname(local_path))
    bucket.download_file(s3_postfix, local_path)
    logger.info(f"downloading file '{s3_path}' -> '{local_path}': done")


#############################################################################

def is_system_path(s3_path):
    for part in s3_path.split("/"):
        if part.startswith("_") or part.startswith("."):
            return True
    return False

def ls(s3_path, recursive=False, hide_system=True, fullpath=True):
    if not s3_path.endswith("/"):
        s3_path += "/"
    s3_bucket, s3_prefix = parse_path(s3_path)
    bucket = boto3.resource("s3").Bucket(s3_bucket)
    objects = bucket.objects.filter(Prefix=s3_prefix)
    results = []
    for obj in objects:
        obj_path = obj.key
        obj_path_full = "s3://" + os.path.join(s3_bucket, obj_path)
        obj_path_parts = obj_path[len(s3_prefix):]
        obj_path_part0 = obj_path_parts.split("/")[0]
        obj_path_part0_full = "s3://" + os.path.join(s3_bucket, s3_prefix, obj_path_part0)
        if hide_system and is_system_path(obj_path_parts):
            continue
        if recursive:
            results.append(obj_path_full) if fullpath else results.append(obj_path_parts)
        else:
            results.append(obj_path_part0_full) if fullpath else results.append(obj_path_part0)
    return sorted(set(results))

#############################################################################

def rm(s3_path):
    logger.info(f"deleting file '{s3_path}'...")
    s3_bucket, s3_postfix = parse_path(s3_path)
    bucket = boto3.resource("s3").Bucket(s3_bucket)
    bucket.delete_objects(Delete={'Objects': [{'Key': s3_postfix}]})
    logger.info(f"deleting file '{s3_path}': done")

#############################################################################
