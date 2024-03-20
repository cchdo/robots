from tempfile import NamedTemporaryFile
import logging
from functools import partial
from base64 import b64encode
from datetime import datetime, timezone
from hashlib import sha256
import argparse
from operator import methodcaller

import xarray as xr
from rich.logging import RichHandler

import cchdo.hydro.accessors  # noqa
from cchdo.auth.session import session as s

logger = logging.getLogger(__name__)

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)


def make_cchdo_file_record(sumfile, fname, file_context):
    return {
        "file": {
            "type": "text/plain",
            "name": fname,
            "body": b64encode(sumfile).decode("ascii"),
        },
        "container_contents": [],
        "data_container": "",
        "data_format": "woce",
        "data_type": "summary",
        "events": [
            {
                "date": datetime.now(tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "name": "CCHDO Website Robot",
                "notes": f"Sumfile generated from {file_context['file_name']} ({file_context['id']})",
                "type": "Generated",
            }
        ],
        "file_hash": sha256(sumfile).hexdigest(),
        "file_name": fname,
        "file_path": "",
        "file_size": len(sumfile),
        "file_sources": [],
        "file_type": "text/plain",
        "other_roles": [],
        "permissions": [],
        "role": "dataset",
        "submissions": [],
    }


def has_cf_file(cruise, files, dtype) -> bool:
    for file in cruise["files"]:
        if (fmeta := files.get(file)) is not None:
            if fmeta["role"] == "dataset" and fmeta["data_type"] == dtype and fmeta["data_format"] == "cf_netcdf":
                return True
    return False

def cf_robot_enabled(cruise, dtype="ctd"):
    if "cf_robots" in cruise and dtype in cruise["cf_robots"]:
        return True

    return False


def is_cf_netcdf_dataset(file) -> bool:
    is_cf = file["data_format"] == "cf_netcdf"
    is_dataset = file["role"] == "dataset"
    return is_cf and is_dataset

def process_single_cruise(cruise, file_by_id, dtype):
    logger.info(f"Processing Cruise: {cruise['expocode']}")
    files = [file_by_id[id] for id in cruise["files"] if id in file_by_id]
    dtype_files_in_dataset = list(filter(lambda f: f["data_type"] == dtype and f["role"] == "dataset", files))
    cf_files = list(filter(lambda f: f["data_format"] == "cf_netcdf",dtype_files_in_dataset))
    non_cf_files = list(filter(lambda f: f["data_format"] != "cf_netcdf",dtype_files_in_dataset))
    if len(cf_files) != 1:
        logger.warning("Found multiple CF files in dataset, this is not implimented yet")
        return
    
    cf_file = cf_files[0]
    cf_file_hash = cf_file["file_hash"]
    files_need_replacing = dict()
    for file in non_cf_files:
        if cf_file_hash in file["file_sources"]:
            continue
        files_need_replacing[file["id"]] = file["data_format"]

    logger.debug(files_need_replacing)
    if any(files_need_replacing):
        file_url = f'https://cchdo.ucsd.edu{cf_file["file_path"]}'

        with NamedTemporaryFile() as tf:
            logger.info(f"Loading {file_url}")
            tf.write(s.get(file_url).content)
            df = xr.load_dataset(tf.name, engine="netcdf4")
        
        for fid, format in files_need_replacing.items():
            func = {
                "woce": methodcaller("to_woce"),
                "whp_netcdf": methodcaller("to_coards"),
                "exchange": methodcaller("to_exchange"),
            }[format]
            try:
                data = func(df.cchdo)
            except Exception:
                logger.error(f"Crash on {format} conversion")
                pass


def examine_dataset_files(cruise, file_by_id, dtype):
    ...

def cruise_add_from_cf(dtype):
    logger.info(f"Checking and converting files for data type: {dtype}")
    logger.info("Loading Cruise and File information")
    cruises = s.get("https://cchdo.ucsd.edu/api/v1/cruise/all").json()
    files = s.get("https://cchdo.ucsd.edu/api/v1/file/all").json()

    cruises_controlled = list(filter(partial(cf_robot_enabled, dtype=dtype), cruises))
    logger.info(f"Found {len(cruises_controlled)} controlled cruises")

    file_by_id = {file["id"]: file for file in files}

    ffunc = partial(has_cf_file, files=file_by_id, dtype=dtype)

    cruises_with_cf = list(filter(ffunc, cruises_controlled))
    for cruise in cruises_with_cf:
        process_single_cruise(cruise, file_by_id=file_by_id, dtype=dtype)

    exit()

    cannot_do = []
    for cruise in cruises_no_sum:
        cf_file = None
        for file_id in cruise["files"]:
            try:
                file = file_by_id[file_id]
            except KeyError:
                continue

            if not is_cf_netcdf_dataset(file):
                continue

            cf_file = file
            break

        if cf_file is None:
            cannot_do.append(cruise)
            continue

        file_url = f'https://cchdo.ucsd.edu{file["file_path"]}'

        with NamedTemporaryFile() as tf:
            logger.info(f"Loading {file_url}")
            tf.write(s.get(file_url).content)
            df = xr.load_dataset(tf.name, engine="netcdf4")
            sumfile = df.cchdo.to_sum()
            logger.info(f"Generated sumfile: \n {sumfile.decode('utf8')[:1000]}[...]")

        submission = make_cchdo_file_record(
            sumfile, f"{cruise['expocode']}su.txt", cf_file
        )

        r = s.post("https://cchdo.ucsd.edu/api/v1/file", json=submission)

        id_ = r.json()["message"].split("/")[-1]

        attach = s.post(
            f'https://cchdo.ucsd.edu/api/v1/cruise/{cruise["id"]}/files/{id_}'
        )

        if not attach.ok:
            logger.critical("Error patching cruise")
            exit(1)

        logger.info(
            f"Cruise {cruise['expocode']} updated with sumfile from {file['file_path']}"
        )

    if len(cannot_do) > 0:
        logger.info(f"Could not generate track for {len(cannot_do)} cruises")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dtype", choices=["bottle", "ctd", "summary"])
    args = parser.parse_args()
    cruise_add_from_cf(dtype=args.dtype)
