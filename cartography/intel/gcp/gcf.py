import logging
import json
from typing import Dict, List
from itertools import groupby
from operator import itemgetter

import neo4j
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from cartography.graph.job import GraphJob
from cartography.models.gcp.gcf import GCPCloudFunctionSchema
from cartography.util import timeit

logger = logging.getLogger(__name__)

@timeit
def get_gcp_cloud_functions(project_id: str, functions_client: Resource) -> List[Dict]:
    """
    Fetches raw GCP Cloud Functions data for a given project across all available locations.
    """
    logger.info(f"Collecting Cloud Functions for project: {project_id}")
    collected_functions = []
    try:
        parent = f"projects/{project_id}/locations/-"
        request = functions_client.projects().locations().functions().list(parent=parent)
        while request is not None:
            response = request.execute()
            if 'functions' in response:
                collected_functions.extend(response['functions'])
            request = functions_client.projects().locations().functions().list_next(
                previous_request=request,
                previous_response=response,
            )
        return collected_functions
    except HttpError as e:
        error_json = json.loads(e.content.decode("utf-8"))
        err = error_json.get("error", {})
        if (
            err.get("status", "") == "PERMISSION_DENIED"
            or (err.get("message") and "API has not been used" in err.get("message"))
        ):
            logger.warning(
                (
                    "Could not retrieve Cloud Functions on project %s due to permissions issues or API not enabled. Code: %s, Message: %s"
                ),
                project_id,
                err.get("code"),
                err.get("message"),
            )
            return []
        else:
            logger.error(
                (
                    "Unhandled HttpError for Cloud Functions in project %s. Code: %s, Message: %s. Raising exception."
                ),
                project_id,
                err.get("code"),
                err.get("message"),
                exc_info=True
            )
            raise

@timeit
def transform_gcp_cloud_functions(functions: List[Dict]) -> List[Dict]:
    """
    Transforms the raw function data to include the region and flatten nested trigger data.
    """
    for func_data in functions:
        func_data['https_trigger_url'] = func_data.get('httpsTrigger', {}).get('url')
        func_data['event_trigger_type'] = func_data.get('eventTrigger', {}).get('eventType')
        func_data['event_trigger_resource'] = func_data.get('eventTrigger', {}).get('resource')
        try:
            func_data['region'] = func_data['name'].split('/')[3]
        except (IndexError, KeyError):
            logger.warning(f"Could not parse region from function name: {func_data.get('name')}")
            func_data['region'] = None
    functions.sort(key=itemgetter('region'))
    return functions


@timeit
def load_gcp_cloud_functions(
    neo4j_session: neo4j.Session,
    data: List[Dict],
    project_id: str,
    update_tag: int,
) -> None:
    """
    Ingests GCP Cloud Functions and their relationships using a single, robust Cypher query.
    """
    logger.info(f"Loading {len(data)} GCP Cloud Functions for project {project_id}.")

    ingest_query = """
    UNWIND $FunctionList as func_data
        MERGE (f:GCPCloudFunction {id: func_data.name})
        ON CREATE SET f.firstseen = timestamp()
        SET
            f.name = func_data.name,
            f.display_name = func_data.displayName,
            f.description = func_data.description,
            f.state = func_data.state,
            f.runtime = func_data.runtime,
            f.entry_point = func_data.entryPoint,
            f.https_trigger_url = func_data.https_trigger_url,
            f.event_trigger_type = func_data.event_trigger_type,
            f.event_trigger_resource = func_data.event_trigger_resource,
            f.create_time = func_data.createTime,
            f.update_time = func_data.updateTime,
            f.service_account_email = func_data.serviceAccountEmail,
            f.project_id = $ProjectId,
            f.region = func_data.region,
            f.lastupdated = $UpdateTag

    WITH f, func_data
    MATCH (p:GCPProject{id: $ProjectId})
    MERGE (f)-[r_res:RESOURCE]->(p)
    ON CREATE SET r_res.firstseen = timestamp()
    SET r_res.lastupdated = $UpdateTag

    WITH f, func_data
    WHERE func_data.serviceAccountEmail IS NOT NULL
    // MERGE the service account to ensure it exists, setting the ID correctly.
    MERGE (sa:GCPServiceAccount{id: func_data.serviceAccountEmail})
    ON CREATE SET sa.firstseen = timestamp()
    SET sa.email = func_data.serviceAccountEmail,
        sa.lastupdated = $UpdateTag

    MERGE (f)-[r_sa:RUNS_AS]->(sa)
    ON CREATE SET r_sa.firstseen = timestamp()
    SET r_sa.lastupdated = $UpdateTag
    """

    for region, functions_in_region in groupby(data, key=itemgetter('region')):
        if region:
            functions_list = list(functions_in_region)
            for func in functions_list:
                func['region'] = region

            neo4j_session.run(
                ingest_query,
                FunctionList=functions_list,
                ProjectId=project_id,
                UpdateTag=update_tag,
            )


@timeit
def cleanup_gcp_cloud_functions(neo4j_session: neo4j.Session, cleanup_job_params: Dict) -> None:
    """
    Deletes stale GCPCloudFunction nodes and their relationships.
    """
    cleanup_job = GraphJob.from_node_schema(GCPCloudFunctionSchema(), cleanup_job_params)
    cleanup_job.run(neo4j_session)



@timeit
def sync(
    neo4j_session: neo4j.Session,
    functions_client: Resource,
    project_id: str,
    update_tag: int,
    common_job_parameters: Dict,
) -> None:
    """
    The main orchestration function to get, transform, load, and clean up GCP Cloud Functions.
    """
    logger.info(f"Syncing GCP Cloud Functions for project {project_id}.")

    functions_data = get_gcp_cloud_functions(project_id, functions_client)
    if functions_data:
        transformed_functions = transform_gcp_cloud_functions(functions_data)
        load_gcp_cloud_functions(neo4j_session, transformed_functions, project_id, update_tag)

    cleanup_job_params = common_job_parameters.copy()
    cleanup_job_params["PROJECT_ID"] = project_id
    cleanup_gcp_cloud_functions(neo4j_session, cleanup_job_params)