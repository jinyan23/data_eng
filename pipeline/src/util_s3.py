#!/usr/bin/env python3

"""Utility functions to access s3"""

import logging
import boto3
import os
from botocore.exceptions import ClientError
import pandas as pd
from io import StringIO


def upload_file(file_name, object_name=None):
    """
    Upload a file to an S3 bucket

    Args:
        file_name: File present in filesystem to upload
        object_name: S3 object name. If not specified then file_name is used

    Returns:
        boolean: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    bucket = os.environ.get('BUCKET')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_fileobj(zip_resp, object_name=None):
    """
    Upload a fileobj to an S3 bucket

    Args:
        resp: File to upload
        object_name: S3 object name

    Returns:
        boolean: True if file was uploaded, else False
    """

    # Stream the file object
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_fileobj(zip_resp,
                                 os.environ.get('BUCKET'),
                                 object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def read_csv_s3(object_name=None, **kwargs):
    """
    Read a csv file into a pandas dataframe

    Args:
        object_name: S3 object name.

    Returns:
        df: pandas dataframe
    """

    client = boto3.client('s3')
    bucket = os.environ.get('BUCKET')
    csv_obj = client.get_object(Bucket=bucket,
                                Key=object_name)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string),
                     **kwargs)

    return df
