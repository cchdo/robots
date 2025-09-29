# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "cchdo-auth==1.0.2",
#     "cchdo-hydro[netcdf]==1.0.2.15",
#     "rich",
# ]
# ///
from tempfile import NamedTemporaryFile
import logging

import xarray as xr
from rich.logging import RichHandler

import cchdo.hydro.accessors  # noqa
from cchdo.auth.session import session as s

logger = logging.getLogger(__name__)

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)


def has_no_track(cruise) -> bool:
    return cruise["geometry"]["track"] == {}


def is_cf_netcdf_dataset(file) -> bool:
    is_cf = file["data_format"] == "cf_netcdf"
    is_dataset = file["role"] == "dataset"
    return is_cf and is_dataset


def cruise_add_cruise_track_from_cf():
    logger.info("Loading Cruise and File information")
    cruises = s.get("https://cchdo.ucsd.edu/api/v1/cruise/all").json()
    files = s.get("https://cchdo.ucsd.edu/api/v1/file/all").json()

    file_by_id = {file["id"]: file for file in files}

    cruises_no_track = list(filter(has_no_track, cruises))
    logger.info(f"{len(cruises_no_track)} of {len(cruises)} cruises have no trackline")

    cannot_do = []
    for cruise in cruises_no_track:
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

        file_url = f"https://cchdo.ucsd.edu{file['file_path']}"

        with NamedTemporaryFile() as tf:
            logger.info(f"Loading {file_url}")
            tf.write(s.get(file_url).content)
            df = xr.load_dataset(tf.name, engine="netcdf4", decode_timedelta=False)
            track = df.cchdo.track

        patch = [{"op": "replace", "path": "/geometry/track", "value": track}]

        logger.info(f"Generated patch {patch}")

        response = s.patch(
            f"https://cchdo.ucsd.edu/api/v1/cruise/{cruise['id']}", json=patch
        )

        if not response.ok:
            logger.critical("Error patching cruise")

        logger.info(
            f"Cruise {cruise['expocode']} updated with trackline from {file['file_path']}"
        )

    if len(cannot_do) > 0:
        logger.info(f"Could not generate track for {len(cannot_do)} cruises")


if __name__ == "__main__":
    cruise_add_cruise_track_from_cf()
