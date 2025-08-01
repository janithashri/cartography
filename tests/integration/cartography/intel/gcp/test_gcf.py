import pytest
from unittest.mock import patch, MagicMock
import neo4j
from typing import Dict, List
from google.auth.credentials import Credentials as GoogleCredentials

import cartography.intel.gcp.gcf as gcf
from cartography.client.core.tx import load
from cartography.models.gcp.iam import GCPServiceAccountSchema

# --- Test Data (Mock GCP API Response) ---
from unittest.mock import MagicMock, patch

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
            "serviceAccountEmail": "service-1@test-project.iam.gserviceaccount.com",
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
            "serviceAccountEmail": "service-2@test-project.iam.gserviceaccount.com",
        },
    ],
}

@patch('cartography.intel.gcp.gcf.get_gcp_cloud_functions')
def test_gcp_functions_load_and_relationships(
    mock_get_functions: MagicMock,
    neo4j_session: neo4j.Session,
):
    """
    Test that we can correctly load GCP Cloud Functions and their relationships to GCPProject.
    """
    # Arrange: Configure the mock
    mock_get_functions.return_value = GCP_FUNCTIONS_RESPONSE["functions"]

    # Arrange: Create the parent GCPProject node
    project_id = "test-project"
    update_tag = 123456789
    neo4j_session.run(
        """
        MERGE (p:GCPProject{id: $PROJECT_ID})
        SET p.lastupdated = $UPDATE_TAG
        """,
        PROJECT_ID=project_id,
        UPDATE_TAG=update_tag,
    )
    
    # DEBUG STEP 1: Confirm project exists before sync
    project_count = neo4j_session.run("MATCH (p:GCPProject) RETURN count(p) as c").single()['c']
    print(f"\n--- DEBUG PRE-SYNC: GCPProject node count is: {project_count} ---")

    # Act: Call the main sync function
    gcf.sync(
        neo4j_session,
        None,  
        project_id,
        update_tag,
        {"UPDATE_TAG": update_tag, "PROJECT_ID": project_id},
    )

    # --- START OF POST-SYNC DEBUGGING BLOCK ---
    print("\n--- DEBUG POST-SYNC: Querying graph state BEFORE asserts ---")

    func_count = neo4j_session.run("MATCH (n:GCPCloudFunction) RETURN count(n) as c").single()['c']
    print(f"--- DEBUG POST-SYNC: GCPCloudFunction node count: {func_count} ---")

    sa_count = neo4j_session.run("MATCH (n:GCPServiceAccount) RETURN count(n) as c").single()['c']
    print(f"--- DEBUG POST-SYNC: GCPServiceAccount node count: {sa_count} ---")

    res_rel_count = neo4j_session.run("MATCH (:GCPProject)<-[:RESOURCE]-(:GCPCloudFunction) RETURN count(*) as c").single()['c']
    print(f"--- DEBUG POST-SYNC: [:RESOURCE] relationship count: {res_rel_count} ---")

    runs_as_rel_count = neo4j_session.run("MATCH (:GCPCloudFunction)-[:RUNS_AS]->(:GCPServiceAccount) RETURN count(*) as c").single()['c']
    print(f"--- DEBUG POST-SYNC: [:RUNS_AS] relationship count: {runs_as_rel_count} ---")

    print("--- END OF DEBUGGING BLOCK ---\n")
    # --- END OF POST-SYNC DEBUGGING BLOCK ---

    # Assert 1: Check that the Cloud Function nodes were created.
    expected_nodes = {
        "projects/test-project/locations/us-central1/functions/function-1",
        "projects/test-project/locations/us-east1/functions/function-2",
    }
    nodes = neo4j_session.run("MATCH (n:GCPCloudFunction) RETURN n.id")
    actual_nodes = {n['n.id'] for n in nodes}
    assert actual_nodes == expected_nodes

    # Assert 2: Check for the relationship to the project.
    rels_to_project = neo4j_session.run(
        """
        MATCH (p:GCPProject{id:$PROJECT_ID})<-[:RESOURCE]-(f:GCPCloudFunction)
        RETURN count(f) AS rel_count
        """,
        PROJECT_ID=project_id,
    )
    assert rels_to_project.single()['rel_count'] == 2