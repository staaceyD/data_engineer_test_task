import boto3
from config import BUCKET_NAME, FILES_LIST
import logging
from botocore.exceptions import ClientError
import json

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
        logger.info("Got object '%s' from bucket '%s'.", object_key, bucket.name)
    except ClientError:
        logger.exception(("Couldn't get object '%s' from bucket '%s'.",
                          object_key, bucket.name))
        raise
    else:
        return body


def get_files(files_name):
    """
    Creates a list of bucket object name files
    :return: list
    """
    
    return get_object(bucket, files_name).decode("utf-8")
    #  file_names.split("\n")
    
def process_files():
    """
    Creates dictionary where key is a filename from the bucket, value is json content of the file
    :return: dictionary
    """

    files = get_files(FILES_LIST).split("\n")
    return {file_name:json.loads(get_files(file_name)) for file_name in files}

        


process_files()