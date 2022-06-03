# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import jsonlines
import json
import gzip
import time

from io import TextIOWrapper, RawIOBase
from aws_lambda_powertools import Logger, Tracer

tracer = Tracer()
logger = Logger(child = True)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

MAX_SQS_BATCH_SIZE = 10

class StreamingBodyIO(RawIOBase):
    """ Wrap a boto StreamingBody in the IOBase API. """
    def __init__(self, body):
        self.body = body

    def readable(self):
        return True

    def read(self, n: int = -1):
        n = None if n < 0 else n
        return self.body.read(n)

@tracer.capture_method
def enqueue_file(bucket: str, key: str, queue_url: str) -> int:
    enqueue_start = time.perf_counter()

    logger.info('Streaming %s from %s S3 bucket', key, bucket)
    response = s3.get_object(Bucket = bucket, Key = key)

    if key.endswith('.gz') or key.endswith('.gzip'):
        stream = gzip.GzipFile(None, 'rb', fileobj = response['Body'])
    else:
        stream = StreamingBodyIO(response['Body'])

    lines_read = 0

    logger.info('Enqueueing records into %s', queue_url)
    reader = jsonlines.Reader(TextIOWrapper(stream))
    entries = []
    for line in reader:
        lines_read += 1

        id = line.get('queryUserId')    # user-personalization
        if not id:
            id = line.get('userId') # related items with user mapping

        entries.append({
            'Id': f'{len(entries)}-{id}',
            'MessageBody': json.dumps(line)
        })

        if len(entries) == MAX_SQS_BATCH_SIZE:
            response = sqs.send_message_batch(QueueUrl = queue_url, Entries = entries)
            entries = []

    if len(entries) > 0:
        response = sqs.send_message_batch(QueueUrl = queue_url, Entries = entries)

    elapsed = time.perf_counter() - enqueue_start

    logger.info('Enqueued %d lines into queue %s in %0.2f seconds', lines_read, queue_url, elapsed)

    return lines_read
