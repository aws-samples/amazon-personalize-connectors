# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import boto3
import logging

from crhelper import CfnResource

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

helper = CfnResource()
s3 = boto3.resource('s3')

def copy_scripts(event):
    source_path = event["ResourceProperties"]["SourcePath"]
    target_path = event["ResourceProperties"]["TargetPath"]

    logger.info("Copying Glue scripts from %s to %s", source_path, target_path)

    source_bucket_name, source_key = source_path.replace("s3://", "").split("/", 1)
    target_bucket_name, target_key = target_path.replace("s3://", "").split("/", 1)

    source_bucket = s3.Bucket(source_bucket_name)
    target_bucket = s3.Bucket(target_bucket_name)

    for obj in source_bucket.objects.filter(Prefix = source_key):
        source = { 'Bucket': source_bucket_name, 'Key': obj.key }
        target = target_bucket.Object(target_key + obj.key[len(source_key):])
        target.copy(source)

@helper.create
def create_resource(event, _):
    copy_scripts(event)

def lambda_handler(event, context):
    logger.info(os.environ)
    logger.info(json.dumps(event, indent = 2, default = str))

    # If the event has a RequestType, we're being called by CFN as custom resource
    if event.get('RequestType'):
        logger.info('Function called from CloudFormation as custom resource')
        helper(event, context)
    else:
        logger.info('Function called outside of CloudFormation')
        # Call function directly (i.e. testing in Lambda console or called directly)
        copy_scripts(event)
