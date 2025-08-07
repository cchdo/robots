# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "cchdo-auth==1.0.2",
#     "cchdo-hydro[netcdf]==1.0.2.13",
#     "rich",
# ]
# ///
from collections import defaultdict
from tempfile import NamedTemporaryFile
import logging
from functools import partial
from base64 import b64encode
from datetime import datetime, timezone
from hashlib import sha256
import argparse
from operator import methodcaller
import warnings
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


@contextmanager
def GHAGroup(group_name: str):
    if ON_GHA:
        print(f"::group::{group_name}")
    yield
    if ON_GHA:
        print("::endgroup::")


logger = logging.getLogger(__name__)

console = Console(color_system="256")

FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG",
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(console=console)],
)

TO_FTPYE = {
    "woce": "woce",
    "exchange": "exchange",
    "whp_netcdf": "coards",
}
TO_FTPYE_MIME = {
    "ctd": {
        "woce": "application/zip",
        "exchange": "application/zip",
        "whp_netcdf": "application/zip",
    },
    "bottle": {
        "woce": "text/plain",
        "exchange": "text/csv",
        "whp_netcdf": "application/zip",
    },
}


def make_cchdo_file_record(
    data: bytes,
    fname,
    file_context,
    mime="text/plain",
    data_format="exchange",
    dtype="ctd",
):
    return {
        "file": {
            "type": mime,
            "name": fname,
            "body": b64encode(data).decode("ascii"),
        },
        "container_contents": [],
        "data_container": "",
        "data_format": data_format,
        "data_type": dtype,
        "events": [
            {
                "date": datetime.now(tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "name": "CCHDO CF Robot",
                "notes": f"Generated from {file_context['file_name']} ({file_context['id']})",
                "type": "Generated",
            }
        ],
        "file_hash": sha256(data).hexdigest(),
        "file_name": fname,
        "file_path": "",
        "file_size": len(data),
        "file_sources": [file_context["file_hash"]],
        "file_type": mime,
        "other_roles": [],
        "permissions": [],
        "role": "dataset",
        "submissions": [],
    }


def gen_merge_patch():
    return [
        {
            "path": "/events/0",
            "op": "add",
            "value": {
                "date": datetime.now(tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "name": "CCHDO CF Robot",
                "notes": "The CF source file was updated so this file was regenerated",
                "type": "Replaced",
            },
        },
        {"path": "/role", "op": "replace", "value": "merged"},
    ]


def has_cf_file(cruise, files, dtype) -> bool:
    for file in cruise["files"]:
        if (fmeta := files.get(file)) is not None:
            if (
                fmeta["role"] == "dataset"
                and fmeta["data_type"] == dtype
                and fmeta["data_format"] == "cf_netcdf"
            ):
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


def get_files_neededing_replacment(cruise, file_by_id, dtype):
    logger.debug(f"Checking {cruise['expocode']}")
    files = [file_by_id[id] for id in cruise["files"] if id in file_by_id]
    dtype_files_in_dataset = list(
        filter(lambda f: f["data_type"] == dtype and f["role"] == "dataset", files)
    )
    cf_files = list(
        filter(lambda f: f["data_format"] == "cf_netcdf", dtype_files_in_dataset)
    )
    non_cf_files = list(
        filter(lambda f: f["data_format"] != "cf_netcdf", dtype_files_in_dataset)
    )
    if len(cf_files) != 1:
        logger.warning(
            f"Cruise  {cruise['expocode']}: Found multiple CF files in dataset, this is not implemented yet"
        )
        return "Multiple CF Files"

    cf_file = cf_files[0]
    cf_file_hash = cf_file["file_hash"]
    files_need_replacing = dict()
    # preloads so things will be created
    for ftype in TO_FTPYE:
        files_need_replacing[ftype] = ftype
    for file in non_cf_files:
        if cf_file_hash in file["file_sources"]:
            del files_need_replacing[file["data_format"]]
            continue
        if len(file["cruises"]) > 1:
            logger.warning(
                f"Cruise  {cruise['expocode']}:File attached to multiple cruises, this is not implemented yet"
            )
            return "Attached to Multiple Cruises"
        files_need_replacing[file["id"]] = file["data_format"]
        del files_need_replacing[file["data_format"]]

    return (cf_file, files_need_replacing)


def process_single_cruise(cruise, dtype, file_by_id, cf_file, files_need_replacing):
    global dirty
    file_url = f"https://cchdo.ucsd.edu{cf_file['file_path']}"
    file_hashes = {file["file_hash"]: id for id, file in file_by_id.items()}

    with NamedTemporaryFile() as tf:
        logger.info(f"Loading {file_url}")
        tf.write(s.get(file_url).content)
        df = xr.load_dataset(tf.name, engine="netcdf4", decode_timedelta=False)

    for fid, format in files_need_replacing.items():
        fname = df.cchdo.gen_fname(TO_FTPYE[format])
        logger.info(f"Converting {file_url} to {format}: {fname}")
        func = {
            "woce": methodcaller("to_woce"),
            "whp_netcdf": methodcaller("to_coards"),
            "exchange": methodcaller("to_exchange"),
        }[format]
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                data: bytes = func(df.cchdo)
        except Exception as err:
            logger.error(f"Crash on {format} conversion")
            logger.error(err)
            dirty = True
            continue
        mime = TO_FTPYE_MIME[dtype][format]
        api_data = make_cchdo_file_record(
            data, fname, cf_file, mime=mime, data_format=format, dtype=dtype
        )
        if api_data["file_hash"] in file_hashes:
            file_updated_patch = [
                {"op": "add", "path": "/file_sources/0", "value": cf_file["file_hash"]}
            ]
            r = s.patch(
                f"https://cchdo.ucsd.edu/api/v1/file/{fid}", json=file_updated_patch
            )
            logger.info(f"updated file source hash for existing file {fid}")
            continue

        r = s.post("https://cchdo.ucsd.edu/api/v1/file", json=api_data)
        if not r.ok:
            dirty = True
            logger.critical("Error uploading file")

        new_id = r.json()["message"].split("/")[-1]
        attach = s.post(
            f"https://cchdo.ucsd.edu/api/v1/cruise/{cruise['id']}/files/{new_id}"
        )

        if not attach.ok:
            dirty = True
            logger.critical("Error patching cruise")

        if isinstance(fid, int):
            file_replaced_patch = gen_merge_patch()
            logger.info(file_replaced_patch)
            r = s.patch(
                f"https://cchdo.ucsd.edu/api/v1/file/{fid}", json=file_replaced_patch
            )
            if not r.ok:
                dirty = True
                logger.critical("Error patching the replaced file")


def cruise_add_from_cf(dtype):
    global dirty
    logger.info(f"Checking and converting files for data type: {dtype}")
    with GHAGroup("Load cruise and file metadata"):
        logger.info("Loading Cruise and File information")
        cruises = s.get("https://cchdo.ucsd.edu/api/v1/cruise/all").json()
        files = s.get("https://cchdo.ucsd.edu/api/v1/file/all").json()

    cruises_controlled = list(filter(partial(cf_robot_enabled, dtype=dtype), cruises))
    logger.info(f"Found {len(cruises_controlled)} controlled cruises")

    file_by_id = {file["id"]: file for file in files}
    cruise_by_expocode = {cruise["expocode"]: cruise for cruise in cruises_controlled}

    ffunc = partial(has_cf_file, files=file_by_id, dtype=dtype)

    cruises_with_cf = list(filter(ffunc, cruises_controlled))

    with GHAGroup("Find cruises that need work"):
        cruise_files_need_replacing = {
            cruise["expocode"]: get_files_neededing_replacment(
                cruise, file_by_id=file_by_id, dtype=dtype
            )
            for cruise in cruises_with_cf
        }

    cruises_with_work = {}
    cruises_nothing_to_do = []
    cruises_errors = defaultdict(list)
    for expocode, result in cruise_files_need_replacing.items():
        if isinstance(result, tuple):
            cf_file, files_need_replacing = result
            if len(files_need_replacing) == 0:
                cruises_nothing_to_do.append(expocode)
                continue
            cruises_with_work[expocode] = result
        else:
            cruises_errors[result].append(expocode)
    with GHAGroup(f"Up to date cruises ({len(cruises_nothing_to_do)})"):
        logger.info(f"Found {len(cruises_nothing_to_do)} cruises that are up to date")
        logger.info(cruises_nothing_to_do)
    for err, expocodes in cruises_errors.items():
        dirty = True
        logger.error(f"Found {len(expocodes)} cruises with error {err}")
        logger.error(expocodes)

    for expocode, result in cruises_with_work.items():
        cruise = cruise_by_expocode[expocode]
        cf_file, files_need_replacing = result
        with GHAGroup(f"Processing cruise {expocode}"):
            process_single_cruise(
                cruise,
                dtype=dtype,
                file_by_id=file_by_id,
                cf_file=cf_file,
                files_need_replacing=files_need_replacing,
            )


if __name__ == "__main__":
    global dirty
    dirty = False
    parser = argparse.ArgumentParser()
    parser.add_argument("dtype", choices=["bottle", "ctd", "summary"])
    args = parser.parse_args()
    cruise_add_from_cf(dtype=args.dtype)
    if dirty:
        exit(1)
