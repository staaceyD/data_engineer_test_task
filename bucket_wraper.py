import json
import logging

import boto3
from botocore.exceptions import ClientError

from config import BUCKET_NAME, FILES_LIST

s3_client = boto3.resource('s3')
bucket = s3_client.Bucket(BUCKET_NAME)

logger = logging.getLogger(__name__)


def get_object(bucket, object_key):
    """
    Gets an object from a bucket.
    :return: The object data in bytes.
    """
    try:
        body = bucket.Object(object_key).get()['Body'].read()
        logger.info("Got object '%s' from bucket '%s'.",
                    object_key, bucket.name)
    except ClientError:
        logger.exception(("Couldn't get object '%s' from bucket '%s'.",
                          object_key, bucket.name))
        raise
    else:
        return body


def get_files(files_name):
    """
    Creates a string of bucket object name files
    :return: string
    """
    return get_object(bucket, files_name).decode("utf-8")


def process_files():
    """
    Create list of dictionaries with filtered data by types
    :return: list
    """
    files_list = get_files(FILES_LIST).split("\n")

    types = ["song", "movie", "app"]
    for file_name in files_list:
        files_json = json.loads(get_files(file_name))

        return [record for record in files_json if record["type"] in types]
