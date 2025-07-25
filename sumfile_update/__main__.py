# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "cchdo-auth==1.0.2",
#     "cchdo-hydro[netcdf]==1.0.2.13",
#     "rich",
# ]
# ///
from tempfile import NamedTemporaryFile
import logging
from functools import partial
from base64 import b64encode
from datetime import datetime, timezone
from hashlib import sha256
import os
from contextlib import contextmanager

import xarray as xr
from rich.logging import RichHandler
from rich.console import Console

import cchdo.hydro.accessors  # noqa
from cchdo.auth.session import session as s

ON_GHA = "GITHUB_RUN_ID" in os.environ

if ON_GHA:
    # closes the group started by the calling run line
    # This group is for the uv installs
    print("::endgroup::")

console = Console(color_system="256")

logger = logging.getLogger(__name__)

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET",
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(console=console)],
)


@contextmanager
def GHAGroup(group_name: str):
    if ON_GHA:
        print(f"::group::{group_name}")
    yield
    if ON_GHA:
        print("::endgroup::")


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


def has_no_sumfile(cruise, files) -> bool:
    for file in cruise["files"]:
        if (fmeta := files.get(file)) is not None:
            if fmeta["role"] == "dataset" and fmeta["data_type"] == "summary":
                return False
    return True


def is_cf_netcdf_dataset(file) -> bool:
    is_cf = file["data_format"] == "cf_netcdf"
    is_dataset = file["role"] == "dataset"
    return is_cf and is_dataset


def cruise_add_sumfile_from_cf():
    with GHAGroup("Load Cruise and File Metadata"):
        logger.info("Loading Cruise and File information")
        cruises = s.get("https://cchdo.ucsd.edu/api/v1/cruise/all").json()
        files = s.get("https://cchdo.ucsd.edu/api/v1/file/all").json()

        file_by_id = {file["id"]: file for file in files}
        file_by_hash = {file["file_hash"]: file for file in files}

        ffunc = partial(has_no_sumfile, files=file_by_id)

        cruises_no_sum = list(filter(ffunc, cruises))
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
            cannot_do.append(cruise["expocode"])
            continue

        file_url = f"https://cchdo.ucsd.edu{file['file_path']}"

        with GHAGroup(f"Generating sumfile for: {cruise['expocode']}"):
            with NamedTemporaryFile() as tf:
                logger.info(f"Loading {file_url}")
                tf.write(s.get(file_url).content)
                df = xr.load_dataset(tf.name, engine="netcdf4")
                sumfile = df.cchdo.to_sum()
                logger.info(
                    f"Generated sumfile: \n {sumfile.decode('utf8')[:1000]}[...]"
                )

            submission = make_cchdo_file_record(
                sumfile, f"{cruise['expocode']}su.txt", cf_file
            )
            if (file := file_by_hash.get(submission["file_hash"])) is not None:
                id_ = file["id"]
                patch = [
                    {"op": "replace", "path": "/role", "value": "dataset"},
                    {"op": "replace", "path": "/data_format", "value": "woce"},
                    {"op": "replace", "path": "/data_format", "value": "woce"},
                    {"op": "replace", "path": "/data_type", "value": "summary"},
                    {"op": "replace", "path": "/file_type", "value": "text/plain"},
                    {
                        "op": "replace",
                        "path": "/file_name",
                        "value": submission["file_name"],
                    },
                ]
                r = s.post(f"https://cchdo.ucsd.edu/api/v1/file/{id_}")
                if not r.ok:
                    logger.critical(f"Could not reactivate file {id_}")
                    exit(1)
                r = s.patch(f"https://cchdo.ucsd.edu/api/v1/file/{id_}", json=patch)
                if not r.ok:
                    logger.critical(f"Could not patch file {id_}")
                    exit(1)

            else:
                r = s.post("https://cchdo.ucsd.edu/api/v1/file", json=submission)

                if not r.ok:
                    logger.critical("Could not create sumfile")
                    exit(1)

                id_ = r.json()["message"].split("/")[-1]

            attach = s.post(
                f"https://cchdo.ucsd.edu/api/v1/cruise/{cruise['id']}/files/{id_}"
            )

            if not attach.ok:
                logger.critical("Error patching cruise")
                exit(1)

            logger.info(
                f"Cruise {cruise['expocode']} updated with sumfile from {cf_file['file_path']}"
            )

    if len(cannot_do) == len(cruises_no_sum):
        logger.info("No sumfiles were generated")

    if len(cannot_do) > 0:
        with GHAGroup("Cruises where a sumfile could not be generated"):
            logger.info(f"Could not generate sumfile for {len(cannot_do)} cruises:")
            logger.info(cannot_do)


if __name__ == "__main__":
    cruise_add_sumfile_from_cf()
