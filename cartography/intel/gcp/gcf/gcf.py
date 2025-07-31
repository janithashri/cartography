import logging
import json
from typing import Dict, List
from itertools import groupby
from operator import itemgetter

import neo4j
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from cartography.client.core.tx import load
from cartography.graph.job import GraphJob
from cartography.models.gcp.gcf import GCPCloudFunctionNode # Assuming your model is models/gcp/gcf.py
from cartography.util import timeit

logger = logging.getLogger(__name__)

@timeit
def get_gcp_cloud_functions(gcp_project_id: str, functions_client: Resource) -> List[Dict]:
    """
    Fetches raw GCP Cloud Functions data for a given project across all available locations.
    """
    collected_functions = []
    try:
        parent = f"projects/{gcp_project_id}/locations/-"
        request = functions_client.projects().locations().functions().list(parent=parent)
        while request is not None:
            response = request.execute()
            if 'functions' in response:
                collected_functions.extend(response['functions'])
            request = functions_client.projects().locations().functions().list_next(
                previous_request=request,
                previous_response=response,
            )
    except HttpError as e:
        error_details = json.loads(e.content.decode('utf-8'))
        reason = error_details.get('error', {}).get('errors', [{}])[0].get('reason')
        message = error_details.get('error', {}).get('message')
        if reason == "accessNotConfigured" or "API has not been used" in message:
            logger.info(f"Cloud Functions API not enabled for project {gcp_project_id}; skipping.")
        elif reason == "forbidden" or "Permission denied" in message:
            logger.warning(f"Permission denied to list Cloud Functions in project {gcp_project_id}; skipping.")
        else:
            logger.error(f"Unhandled HttpError for Cloud Functions in project {gcp_project_id}: {e}")
            raise
    return collected_functions

@timeit
def transform_gcp_cloud_functions(functions: List[Dict]) -> List[Dict]:
    """
    Transforms the raw function data to include the region for grouping.
    """
    for func_data in functions:
        try:
            func_data['region'] = func_data['name'].split('/')[3]
        except (IndexError, KeyError):
            logger.warning(f"Could not parse region from function name: {func_data.get('name')}")
            func_data['region'] = None
    functions.sort(key=itemgetter('region')) # Sort by region for groupby to work correctly
    return functions

@timeit
def load_gcp_cloud_functions(
    neo4j_session: neo4j.Session,
    data: List[Dict],
    project_id: str,
    update_tag: int,
) -> None:
    """
    Loads GCP Cloud Functions into Neo4j using the pre-defined schema.
    """
    logger.info(f"Loading {len(data)} GCP Cloud Functions for project {project_id}.")
    
    # Group functions by region before loading for more efficient Cypher execution
    for region, functions_in_region in groupby(data, key=itemgetter('region')):
        if region: # Only process if region was successfully extracted
            functions_list = list(functions_in_region)
            load(
                neo4j_session,
                GCPCloudFunctionNode(), # Your defined schema
                functions_list,
                lastupdated=update_tag,
                PROJECT_ID=project_id, # Passed as kwarg for relationship matching
                region=region, # Passed as kwarg for node property
            )

@timeit
def cleanup_gcp_cloud_functions(neo4j_session: neo4j.Session, cleanup_job_params: Dict) -> None:
    """
    Deletes stale GCPCloudFunction nodes and their relationships.
    """
    cleanup_job = GraphJob.from_node_schema(GCPCloudFunctionNode(), cleanup_job_params)
    cleanup_job.run(neo4j_session)

@timeit
def sync(
    neo4j_session: neo4j.Session,
    functions_client: Resource,
    gcp_project_id: str,
    update_tag: int,
    common_job_parameters: Dict,
) -> None:
    """
    The main orchestration function to get, transform, load, and clean up GCP Cloud Functions.
    """
    logger.info(f"Syncing GCP Cloud Functions for project {gcp_project_id}.")
    
    functions_data = get_gcp_cloud_functions(gcp_project_id, functions_client) 
    if functions_data:
        transformed_functions = transform_gcp_cloud_functions(functions_data)
        load_gcp_cloud_functions(neo4j_session, transformed_functions, gcp_project_id, update_tag)

    cleanup_job_params = common_job_parameters.copy()
    cleanup_job_params["PROJECT_ID"] = gcp_project_id
    cleanup_gcp_cloud_functions(neo4j_session, cleanup_job_params)