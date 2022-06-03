# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import os
import requests
from typing import List

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.parameters import get_parameters

tracer = Tracer()
logger = Logger()

PARAM_PREFIX = os.environ['PARAM_PREFIX']
PARAM_REST_ENDPOINT_URL = PARAM_PREFIX + 'rest-endpoint-url'
PARAM_API_KEY = PARAM_PREFIX + 'api-key'

MAX_ATTRIBUTES_PER_REQUEST = 75 # https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#rate-limit

braze_rest_endpoint_url = None
braze_api_key = None

params = get_parameters(PARAM_PREFIX)
for key, value in params.items():
    if PARAM_PREFIX + key == PARAM_REST_ENDPOINT_URL:
        braze_rest_endpoint_url = value
    if PARAM_PREFIX + key == PARAM_API_KEY:
        braze_api_key = value

if braze_rest_endpoint_url is None:
    raise Exception(f'Braze REST Endpoint URL not set in SSM parameter "{PARAM_REST_ENDPOINT_URL}"')
if braze_api_key is None:
    raise Exception(f'Braze API Key not set in SSM parameter "{PARAM_API_KEY}"')

def call_braze_user_track(attributes: List):
    data = { 'attributes': attributes }

    headers = {
        'Content-type': 'application/json',
        'Authorization': 'Bearer ' + braze_api_key,
        'X-Braze-Bulk': 'true'
    }

    logger.debug('Calling Braze with: %s', data)
    requests.post(braze_rest_endpoint_url + '/users/track', json=data, headers=headers)

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, _):
    if event.get('Records'):
        attributes = []

        for record in event['Records']:
            document = json.loads(record["body"])
            logger.debug(document)

            user_id = document['queryUserId'] if 'queryUserId' in document else document.get('userId')
            if not user_id:
                logger.error('Record is missing "queryUserId" and "userId" fields; ignoring')
                continue

            recommendations = document.get('recommendations')
            if not recommendations:
                logger.error('Record is missing "recommendations" field; ignoring')
                continue

            attribute = {
                'external_id': str(user_id)
            }

            sync_directives = document.get('syncDirectives')
            prefix = sync_directives['attributePrefix'] if sync_directives and 'attributePrefix' in sync_directives else 'recommendation_'
            other_attribs = sync_directives['otherAttributes'] if sync_directives and 'otherAttributes' in sync_directives else None
            if other_attribs:
                other_attribs.pop('external_id', None)
                attribute.update(other_attribs)

            for recommendation in recommendations:
                for key, value in recommendation.items():
                    attrib_key = prefix + key
                    attribute.setdefault(attrib_key, []).append(value)

            attributes.append(attribute)

            if len(attributes) == MAX_ATTRIBUTES_PER_REQUEST:
                call_braze_user_track(attributes)
                attributes = []

        if len(attributes) > 0:
            call_braze_user_track(attributes)
    else:
        logger.error('Invalid/unsupported event; missing "Records"')