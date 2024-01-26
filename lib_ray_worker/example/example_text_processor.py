import csv
import logging
from typing import Dict, Optional

LOGGER = logging.getLogger(__name__)


def process(
    file_path,
    *,
    file_metadata: Optional[Dict] = None,
    field1_param: Optional[str],
    **kwargs,
) -> None:
    """Handles the workflow for an incoming image.
    :param file_path: The file_path to the export file to process.
    :param file_metadata: The properties associated with the incoming file
    :param kwargs: Additional arguments used when creating the requests.Session object for the Rubicon REST client.
    """

    # Allows users to overwrite defaults used in the method

    try:
        print(f"got file {file_path}")

        print(f"got metadata {file_metadata}")
        print(f"got field1_param {field1_param}")

        # TODO do some processing here
        col_mapping = ["timestamp", "longitude", "latitude", "elevation", "data_id"]
        with open(file_path) as f:
            data_parser = csv.DictReader(f, fieldnames=col_mapping, delimiter=",")
            for row in data_parser:
                print(row)

    except Exception as exc:
        LOGGER.exception(f"Failed to process the file {file_path} due to {exc}")
        raise
