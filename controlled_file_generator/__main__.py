from tempfile import NamedTemporaryFile
import logging
from functools import partial
from base64 import b64encode
from datetime import datetime, timezone
from hashlib import sha256
import argparse

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

def process_single_cruise(cruise, file_by_id):
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
    logger.debug(cruises_with_cf)
    exit()
    logger.info(f"{len(cruises_no_sum)} of {len(cruises)} cruises have no sumfile")

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
