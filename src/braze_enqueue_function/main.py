# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import urllib
import os
import re

from aws_lambda_powertools import Logger, Tracer
from enqueue import enqueue_file

tracer = Tracer()
logger = Logger()

s3 = boto3.client('s3')

QUEUE_URL = os.environ['QUEUE_URL']
# Key format should "etl_jobs/<job_type>/<job_name>/output/braze/year=YYYY/month=MM/day=DD/time=HHMMSS/
KEY_MATCH = re.compile("^etl_jobs\/[a-zA-Z0-9!_.*'()-]+\/[a-zA-Z0-9!_.*'()-]+\/output\/braze\/year\=[0-9]{4}\/month\=[0-1]{1}[0-9]{1}\/day\=[0-9]{2}\/time\=[0-9]{6}\/.*")

@tracer.capture_method
def process_event_record(record):
    bucket = record['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(record['s3']['object']['key'])

    if record['s3']['object']['size'] == 0:
        logger.warn('Object is empty (size is zero); ignoring event record')
        return

    if not KEY_MATCH.match(key):
        logger.warn('Object key does not match expected pattern for this function; ignoring event record')
        return

    enqueue_file(bucket, key, QUEUE_URL)

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, _):
    if event.get('Records'):
        for record in event['Records']:
            if record.get('s3'):
                try:
                    process_event_record(record)
                except Exception as e:
                    logger.exception(e)
            else:
                logger.error('Event Record does not appear to be for an S3 event; missing "s3" details')
    else:
        logger.error('Invalid/unsupported event; missing "Records"')