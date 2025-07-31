# Google Cloud Functions Schema

This document describes the graph schema created by the Google Cloud Functions ingestion module.

## Nodes

### `GCPCloudFunction`
Represents a Google Cloud Function.

**Properties:**
- `id` (string, Neo4j ID): Unique identifier for the Cloud Function. Formatted as `projects/{PROJECT_ID}/locations/{REGION}/functions/{FUNCTION_NAME}`.
- `name` (string): Full resource name of the Cloud Function.
- `display_name` (string, optional): User-friendly name.
- `description` (string, optional): Description of the function.
- `runtime` (string): The runtime used by the function (e.g., `python39`, `nodejs16`).
- `entry_point` (string): The name of the function in your code that will be executed.
- `status` (string): The current state of the function (e.g., `ACTIVE`, `FAILED`).
- `create_time` (timestamp): When the function was created.
- `update_time` (timestamp): When the function was last updated.
- `https_trigger_url` (string, optional): The URL if the function is HTTP-triggered.
- `event_trigger_type` (string, optional): The type of event that triggers the function (e.g., `google.cloud.pubsub.topic.v1.messagePublished`).
- `event_trigger_resource` (string, optional): The resource that triggers the function (e.g., Pub/Sub topic name, GCS bucket name).
- `project_id` (string): The ID of the GCP Project this function belongs to.
- `region` (string): The GCP region where the function is deployed.
- `lastupdated` (timestamp): The timestamp of the last successful sync.

## Relationships

### `(:GCPCloudFunction)-[:RESOURCE]->(:GCPProject)`
- **Description:** A `GCPCloudFunction` is a `RESOURCE` belonging to a `GCPProject`. This relationship is crucial for hierarchical scoping and cleanup.
- **Direction:** `INWARD` (from `GCPCloudFunction` to `GCPProject`).