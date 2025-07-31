import pytest
from unittest.mock import patch, MagicMock
import neo4j
from typing import Dict, List
from operator import itemgetter
from itertools import groupby

import cartography.intel.gcp.gcf as gcf

from cartography.models.gcp.gcf import GCPCloudFunctionNode

from cartography.intel.gcp.util import get_gcp_credentials 
import googleapiclient.discovery # Needed for mocking the build() method


GCP_FUNCTIONS_RESPONSE = {
    "functions": [
        {
            "name": "projects/test-project/locations/us-central1/functions/function-1",
            "displayName": "Function One",
            "state": "ACTIVE",
            "runtime": "python310",
            "entryPoint": "hello_world_http",
            "httpsTrigger": {
                "url": "https://us-central1-test-project.cloudfunctions.net/function-1",
            },
            "createTime": "2023-01-01T10:00:00Z",
            "updateTime": "2023-01-01T10:00:00Z",
        },
        {
            "name": "projects/test-project/locations/us-east1/functions/function-2",
            "displayName": "Function Two",
            "state": "ACTIVE",
            "runtime": "nodejs16",
            "entryPoint": "handler_event",
            "eventTrigger": {
                "eventType": "google.cloud.pubsub.topic.v1.messagePublished",
                "resource": "projects/test-project/topics/my-topic",
            },
            "createTime": "2023-02-01T11:00:00Z",
            "updateTime": "2023-02-01T11:00:00Z",
        },
    ],
}


@patch('cartography.intel.gcp.util.get_gcp_credentials') # Patch the credentials getter
@patch('cartography.intel.gcp.gcf._get_cloudfunctions_resource') # Patch the client getter inside gcf.py
def test_gcp_functions_load_and_relationships(
    mock_get_cloudfunctions_resource: MagicMock,
    mock_get_gcp_credentials: MagicMock,
    neo4j_session: neo4j.Session,
):
    """
    Test that we can correctly load GCP Cloud Functions and their relationships to GCPProject.
    """
    # Arrange: Configure the mocks
    mock_get_gcp_credentials.return_value = MagicMock(spec=googleapiclient.discovery.build) 

    mock_functions_client = MagicMock()
    mock_get_cloudfunctions_resource.return_value = mock_functions_client
    
    mock_list_response = MagicMock()
    mock_list_response.get.side_effect = lambda k, default=None: GCP_FUNCTIONS_RESPONSE.get(k, default)
    mock_list_response.__contains__.side_effect = lambda key: key in GCP_FUNCTIONS_RESPONSE
    
    mock_functions_client.projects.return_value.locations.return_value.functions.return_value.list.return_value.execute.return_value = mock_list_response
    mock_functions_client.projects.return_value.locations.return_value.functions.return_value.list_next.return_value = None


    # Arrange: Create the parent GCPProject node in the test database.
    project_id = "test-project"
    neo4j_session.run(
        "MERGE (p:GCPProject{id: $PROJECT_ID})",
        PROJECT_ID=project_id,
    )
    update_tag = 123456789

    # Act: Call the main sync function from your ingestor module.
    gcf.sync(
        neo4j_session,
        project_id,
        update_tag,
        {"UPDATE_TAG": update_tag, "PROJECT_ID": project_id}, # common_job_parameters
    )

    # Assert 1: Check that the Cloud Function nodes were created.
    expected_nodes = {
        "projects/test-project/locations/us-central1/functions/function-1",
        "projects/test-project/locations/us-east1/functions/function-2",
    }
    nodes = neo4j_session.run("MATCH (n:GCPCloudFunction) RETURN n.id")
    actual_nodes = {n['n.id'] for n in nodes}
    assert actual_nodes == expected_nodes

    # Assert 2: Check that the functions are correctly connected to the project via [:RESOURCE] relationship.
    rels = neo4j_session.run(
        """
        MATCH (p:GCPProject{id:$PROJECT_ID})<-[:RESOURCE]-(f:GCPCloudFunction)
        RETURN count(f) AS rel_count
        """,
        PROJECT_ID=project_id,
    )
    assert rels.single()['rel_count'] == 2

    # Assert 3: Check properties for one of the functions to ensure data was loaded correctly.
    func1_data = neo4j_session.run(
        """
        MATCH (f:GCPCloudFunction{id: "projects/test-project/locations/us-central1/functions/function-1"})
        RETURN f.runtime, f.https_trigger_url, f.region, f.createTime, f.updateTime
        """
    ).single()
    assert func1_data['f.runtime'] == "python310"
    assert func1_data['f.https_trigger_url'] == "https://us-central1-test-project.cloudfunctions.net/function-1"
    assert func1_data['f.region'] == "us-central1"
    assert func1_data['f.createTime'] == "2023-01-01T10:00:00Z"
    assert func1_data['f.updateTime'] == "2023-01-01T10:00:00Z"