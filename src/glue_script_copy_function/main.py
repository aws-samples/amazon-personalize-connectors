import os
import json
import boto3
import logging

from crhelper import CfnResource

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

helper = CfnResource()
s3 = boto3.resource('s3')
glue = boto3.client('glue')

def update_job(event):
    job_name = event['ResourceProperties']['JobName']
    target_path = event["ResourceProperties"]["TargetPath"]

    logger.info("Fetching job details for job %s", job_name)
    response = glue.get_job(
        JobName=job_name
    )
    logger.debug(response["Job"])

    script_location = response["Job"]["Command"]["ScriptLocation"]

    if not script_location.startswith("s3://aws-sam-cli-managed"):
        logger.info("Script location does not appear to be SAM CLI bucket; leaving ScriptLocation as is")
        return

    source_bucket, source_key = script_location.replace("s3://", "").split("/", 1)
    target_bucket, target_key = target_path.replace("s3://", "").split("/", 1)

    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }

    bucket = s3.Bucket(target_bucket)

    logger.info("Copying %s to %s", script_location, target_path)
    bucket.copy(copy_source, target_key)

    logger.info("Updating ScriptLocation for %s to point to target path %s", job_name, target_path)
    job = response["Job"]
    job.pop("Name", None)
    job.pop("CreatedOn", None)
    job.pop("LastModifiedOn", None)
    if job.get("MaxCapacity"):
        job.pop("AllocatedCapacity", None)
        job.pop("WorkerType", None)
        job.pop("NumberOfWorkers", None)
    job["Command"]["ScriptLocation"] = target_path

    response = glue.update_job(JobName = job_name, JobUpdate = job)
    logger.debug(response)
    logger.info("Job %s successfully updated", job_name)

@helper.create
@helper.update
def create_or_update_resource(event, _):
    update_job(event)

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
        update_job(event)
