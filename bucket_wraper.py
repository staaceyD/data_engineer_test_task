import json
import logging

import boto3
from botocore.exceptions import ClientError

from constants.bucket_constants import BUCKET_NAME, FILES_LIST

s3_client = boto3.resource('s3')
bucket = s3_client.Bucket(BUCKET_NAME)

logger = logging.getLogger(__name__)


def get_files_json(file_name):
    """
    Gets an object from a bucket.
    :return: The object data in bytes.
    """
    try:
        body = bucket.Object(file_name).get()['Body'].read().decode("utf-8")
        logger.info("Got object '%s' from bucket '%s'.",
                    file_name, bucket.name)
    except ClientError:
        logger.exception(("Couldn't get object '%s' from bucket '%s'.",
                          file_name, bucket.name))
        raise
    else:
        return body


def process_files():
    """
    Create list of dictionaries with filtered data by types
    :return: list
    """
    files_list = get_files_json(FILES_LIST).split("\n")

    types = ["song", "movie", "app"]
    for file_name in files_list:
        files = json.loads(get_files_json(file_name))

        return [record for record in files if record["type"] in types]
