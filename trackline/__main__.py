from tempfile import NamedTemporaryFile
import json

from chalice import Chalice, Cron
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
import xarray as xr
import requests

import cchdo.hydro.accessors  # noqa
from cchdo.auth import CCHDOAuth

logger = Logger()

secrets_provider = parameters.SecretsProvider()

app = Chalice(app_name='auto_track')

def has_no_track(cruise) -> bool:
    return cruise["geometry"]["track"] == {}

def is_cf_netcdf_dataset(file) -> bool:
    is_cf = file["data_format"] == "cf_netcdf"
    is_dataset = file["role"] == "dataset"
    return is_cf and is_dataset

@app.schedule(Cron('0', '12', '*', '*', '?', '*'))  # Execute very day at 12PM
def cruise_add_cruise_track_from_cf(event):

    logger.info("Getting CCHDO API Key")
    api_key = json.loads(secrets_provider.get("cchdo_robot_api_key"))
    cchdo_auth = CCHDOAuth(api_key["cchdo_robot_api_key"])

    logger.info("Loading Cruise and File information")
    cruises = requests.get("https://cchdo.ucsd.edu/api/v1/cruise/all", auth=cchdo_auth).json()
    files = requests.get("https://cchdo.ucsd.edu/api/v1/file/all", auth=cchdo_auth).json()

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

        file_url = f'https://cchdo.ucsd.edu{file["file_path"]}'

        with NamedTemporaryFile() as tf:
            logger.info(f"Loading {file_url}")
            tf.write(requests.get(file_url, auth=cchdo_auth).content)
            df = xr.load_dataset(tf.name, engine="netcdf4")
            track = df.cchdo.track

        patch = [{
            "op": "replace",
            "path": "/geometry/track",
            "value": track
        }]

        logger.info(f"Generated patch {patch}")

        response = requests.patch(f'https://cchdo.ucsd.edu/api/v1/cruise/{cruise["id"]}', json=patch, auth=cchdo_auth)

        if not response.ok:
            logger.critical("Error patching cruise")

        logger.info(f"Cruise {cruise['expocode']} updated with trackline from {file['file_path']}")

    if len(cannot_do) > 0:
        logger.info(f"Could not generate track for {len(cannot_do)} cruises")