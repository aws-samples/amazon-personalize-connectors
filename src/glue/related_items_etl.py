# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import sys
import boto3
import botocore
from datetime import datetime
from typing import Dict
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

REQUIRED_ARGS = [
    "JOB_NAME",
    "S3_JOB_PATH"
]

JOB_TYPE = "related_items"

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

sc = SparkContext()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
job_run_datetime = datetime.today()

s3 = boto3.resource('s3')

def s3_object_or_folder_exists(s3_path: str) -> bool:
    """ Returns True if an S3 folder or object exists, otherwise False. """
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    if key.endswith('/'):
        resp = s3.meta.client.list_objects(Bucket=bucket, Prefix=key, Delimiter='/',MaxKeys=1)
        return 'Contents' in resp
    else:
        try:
            s3.Object(bucket, key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise

# Format: s3://bucket/etl_jobs/related_items/<job_name>
s3_job_path = args["S3_JOB_PATH"]
logger.info(f"S3_JOB_PATH = {s3_job_path}")
if not s3_job_path.endswith("/"):
    s3_job_path += "/"
if not s3_object_or_folder_exists(s3_job_path):
    raise Exception(f"S3_JOB_PATH {s3_job_path} does not exist or is not a folder")

# Validate folder/path
s3_job_bucket, s3_job_base_key = s3_job_path.replace("s3://", "").split("/", 1)
s3_job_base_key_bits = s3_job_base_key.split('/')
if len(s3_job_base_key_bits) < 3 or s3_job_base_key_bits[0] != "etl_jobs" or s3_job_base_key_bits[1] != JOB_TYPE:
    raise Exception(f"S3_JOB_PATH is invalid for this Job type: key must be 'etl_jobs/{JOB_TYPE}/<job_name>/")

etl_job_name = s3_job_base_key_bits[2]

# Load config file
config_file_key = f"etl_jobs/{JOB_TYPE}/{etl_job_name}/config.json"
config_file_path = f"s3://{s3_job_bucket}/{config_file_key}"
if not s3_object_or_folder_exists(config_file_path):
    raise Exception(f"Job configuration file does not exist: {config_file_path}")

logger.info(f"Loading job config file: {config_file_path}")
config = json.loads(s3.Object(s3_job_bucket, config_file_key).get()['Body'].read().decode('utf-8'))
batch_inference_output_path = config.get("batchInferencePath")
if not batch_inference_output_path:
    raise Exception(f"Job configuration file is missing required field 'batchInferencePath' which should point to output S3 path for Personalize related items batch inference job")
logger.info(f"Batch inference path: {batch_inference_output_path}")
if not s3_object_or_folder_exists(batch_inference_output_path):
    raise Exception(f"Batch inference path specified in the job configuration file does not exist: {batch_inference_output_path}")

connector_configs = config.get("connectors")
if not connector_configs:
    raise Exception(f"Job configuration file does not include any connector configurations")

user_item_mapping_csv_path = f"s3://{s3_job_bucket}/etl_jobs/{JOB_TYPE}/{etl_job_name}/input/user_item_mapping/"
logger.info(f"User-Item mapping CSV path: {user_item_mapping_csv_path}")
item_metadata_path = f"s3://{s3_job_bucket}/etl_jobs/{JOB_TYPE}/{etl_job_name}/input/item_metadata/"
logger.info(f"Item metadata path: {item_metadata_path}")
job_output_root_path = f"s3://{s3_job_bucket}/etl_jobs/{JOB_TYPE}/{etl_job_name}/output/"
logger.info(f"Job output root path: {job_output_root_path}")

# Load the output from the Personalize batch inference job
logger.info(f"Loading batch inference output file from {batch_inference_output_path}")
batch_inference_full_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [ batch_inference_output_path ]
    },
    transformation_ctx="load_batch_inference",
)
logger.info(f"Loaded {batch_inference_full_dyf.count()} documents from {batch_inference_output_path}")

# Isolate docs/rows without errors
related_items_recs_df = batch_inference_full_dyf.toDF().where("error IS NULL")
logger.info(f"Isolated {related_items_recs_df.count()} documents without errors")

if config.get("saveBatchInferenceErrors", False):
    # Isolate docs/rows with errors
    errors_df = batch_inference_full_dyf.toDF().where("error IS NOT NULL")
    logger.info(f"Isolated {errors_df.count()} documents with errors")
    if errors_df.count() > 0:
        errors_output_path = f"s3://{s3_job_bucket}/etl_jobs/{JOB_TYPE}/{etl_job_name}/errors/year={job_run_datetime:%Y}/month={job_run_datetime:%m}/day={job_run_datetime:%d}/time={job_run_datetime:%H%M%S}/"

        errors_dyf = DynamicFrame.fromDF(errors_df, glueContext, "ErrorsIsolated")

        logger.info(f'Writing isolated batch inference errors to {errors_output_path}')
        glueContext.write_dynamic_frame.from_options(
            frame=errors_dyf,
            connection_type="s3",
            format="json",
            connection_options={
                "path": errors_output_path,
                "partitionKeys": [],
            },
            transformation_ctx="write_isolated_errors_output"
        )

# Convert to DynamicFrame and drop the "error" field/column (it's null anyway)
related_items_recs_dyf = DynamicFrame.fromDF(related_items_recs_df, glueContext, "RelatedItemsRecs").drop_fields(['error'])

# Load the user/item mapping CSV that allows us to connect related item
# recommendations back to users.
logger.info(f"Loading user/item mapping from {user_item_mapping_csv_path}")
user_item_mapping_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [ user_item_mapping_csv_path ],
        "recurse": True,
    },
    transformation_ctx="load_user_item_mapping",
)
logger.info(f"Loaded {user_item_mapping_dyf.count()} rows from {user_item_mapping_csv_path}")

# Join the batch inference and user/item mapping frames by itemId.
batch_inference_merged_dyf = Join.apply(
    frame1=related_items_recs_dyf,
    frame2=user_item_mapping_dyf,
    keys1=["input.itemId"],
    keys2=["ITEM_ID"],
    transformation_ctx="join_batch_inference_and_user_item_mapping",
)

# Drop the redundant ITEM_ID field
batch_inference_merged_dyf = DropFields.apply(
    frame = batch_inference_merged_dyf,
    paths = "ITEM_ID",
    transformation_ctx="drop_redundant_item_id",
)

# Load item metadata if present in the job path.
item_metadata_df = None
if s3_object_or_folder_exists(item_metadata_path):
    logger.info(f"Loading item metadata from {item_metadata_path}")
    item_metadata_dyf = glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [ item_metadata_path ]
        },
        transformation_ctx="load_item_metadata",
    )
    logger.info(f"Loaded {item_metadata_dyf.count()} documents from {item_metadata_path}")

    item_metadata_df = item_metadata_dyf.toDF()

def decorate_items(batch_inference_df: DataFrame, connector_config: Dict, item_metadata_df: DataFrame) -> DataFrame:
    if item_metadata_df:
        logger.info("Decorating items with item metadata")

        item_metadata_aliased_fields = [ "meta.*" ]
        item_metadata_fields = connector_config.get("itemMetadataFields")
        if item_metadata_fields:
            item_metadata_aliased_fields = ["meta." + x.strip() for x in item_metadata_fields]

        item_metadata_aliased_fields.append(F.col("recItemId").alias("itemId"))

        w = Window.partitionBy('queryItemId').orderBy('pos')

        decorated_df = (batch_inference_df
            .select(F.col("input.itemId").alias("queryItemId"), F.col("USER_ID").alias("userId"), F.posexplode_outer("output.recommendedItems"))
            .withColumnRenamed("col", "recItemId")
            .join(item_metadata_df.alias("meta"), F.col("recItemId") == F.col("id"), "left_outer")
            .withColumn("recItem",
                        F.when(F.col("recItemId").isNull(), None).otherwise(
                            F.struct(item_metadata_aliased_fields)
                            ))
            .withColumn("recommendations", F.collect_list('recItem').over(w))
            .groupBy(['queryItemId','userId'])
            .agg(F.max('recommendations').alias('recommendations')))
    else:
        logger.info('No item metadata file specified in job parametes; skipping decoration of items with item metadata')

        w = Window.partitionBy('queryItemId').orderBy('pos')

        decorated_df = (batch_inference_df
            .select(F.col("input.itemId").alias("queryItemId"), F.col("USER_ID").alias("userId"), F.posexplode_outer("output.recommendedItems"))
            .withColumnRenamed("col", "recItemId")
            .withColumn("recItem",
                        F.when(F.col("recItemId").isNull(), None).otherwise(
                            F.struct(
                                F.col("recItemId").alias("itemId")
                            )))
            .withColumn("recommendations", F.collect_list('recItem').over(w))
            .groupBy(['queryItemId','userId'])
            .agg(F.max('recommendations').alias('recommendations')))

    return decorated_df

# Build output for each of the connector types in the configuration file.
batch_inference_merged_df = batch_inference_merged_dyf.toDF()

for connector_type, connector_config in connector_configs.items():
    logger.info(f"Processing connector type {connector_type}")

    # Decorate items with item metadata based on connector config
    decorated_df = decorate_items(batch_inference_merged_df, connector_config, item_metadata_df)

    perform_delta_check = connector_config.get("performDeltaCheck")

    state_sync_path = f"{job_output_root_path}{connector_type}/state/"

    # We do the delta check after the items are decorated in case we need to update
    # recommendations based on item metadata changes only vs just changed items alone.
    if perform_delta_check and s3_object_or_folder_exists(state_sync_path):
        logger.info(f"Loading last sync state data from {state_sync_path}")
        last_sync_dyf = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            format="json",
            connection_options={
                "paths": [ state_sync_path ]
            },
            multiLine = True
        )

        if last_sync_dyf.count() > 0:
            last_sync_df = last_sync_dyf.toDF()
            last_sync_df = last_sync_df.select(sorted(last_sync_df.columns,reverse=False))

            decorated_df = decorated_df.select(sorted(decorated_df.columns,reverse=False))

            decorated_df = decorated_df.subtract(last_sync_df)
            logger.info(f"Delta of prior sync file for {connector_type} with current file yielded {decorated_df.count()} changes")
        else:
            logger.info(f"Prior sync file is empty for connector {connector_type}")
    else:
        logger.info(f"Delta check disabled or prior sync file(s) do not exist for connector {connector_type}, skipping diff check")

    attribute_prefix = connector_config.get("attributePrefix")
    other_attributes = connector_config.get("otherAttributes")

    # Add job and sync directive information to the dataframe.
    def add_job_and_sync_info(rec):
        rec["jobInfo"] = {}
        rec["jobInfo"]["name"] = args["JOB_NAME"]
        rec["jobInfo"]["runDateTime"] = job_run_datetime

        if attribute_prefix or other_attributes:
            rec["syncDirectives"] = {}
            if attribute_prefix:
                rec["syncDirectives"]["attributePrefix"] = attribute_prefix
            if other_attributes:
                rec["syncDirectives"]["otherAttributes"] = other_attributes

        return rec

    batch_inference_merged_dyf = DynamicFrame.fromDF(decorated_df, glueContext, f"DecoratedToDynamicFrame-{connector_type}")

    batch_inference_merged_dyf = Map.apply(
        frame = batch_inference_merged_dyf,
        f = add_job_and_sync_info,
        transformation_ctx=f"add_job_and_sync_info-{connector_type}"
    )

    # Write the frame back to S3 where file(s) will be picked up by
    # the next consumer.
    if not job_output_root_path.endswith('/'):
        job_output_root_path += '/'
    job_output_path = f"{job_output_root_path}{connector_type}/year={job_run_datetime:%Y}/month={job_run_datetime:%m}/day={job_run_datetime:%d}/time={job_run_datetime:%H%M%S}/"

    logger.info(f'Writing output to {job_output_path}')
    glueContext.write_dynamic_frame.from_options(
        frame=batch_inference_merged_dyf,
        connection_type="s3",
        format="json",
        connection_options={
            "path": job_output_path,
            "partitionKeys": [],
        },
        transformation_ctx=f"write_output-{connector_type}"
    )

logger.info("Commiting job")
job.commit()
