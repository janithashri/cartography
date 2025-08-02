import logging
import time
import neo4j
from cartography.intel.gcp import gcf
from cartography.intel.gcp import get_gcp_credentials, _get_gcf_resource

# --- CONFIGURE YOUR DETAILS HERE ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "jan"  # Replace with your Neo4j password
GCP_PROJECT_ID = "rare-theater-467513-a0" # Replace with your GCP Project ID
# --- END CONFIGURATION ---

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Attempting to get GCP credentials...")
    try:
        credentials = get_gcp_credentials()
    except Exception as e:
        logger.error(f"Could not get GCP credentials. Details: {e}")
        return

    logger.info("Building GCP and Neo4j clients...")
    functions_client = _get_gcf_resource(credentials)
    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        update_tag = int(time.time() * 1000)

        # STEP 1: Ensure the parent GCPProject node exists.
        logger.info("Creating parent GCPProject node...")
        session.run(
            "MERGE (p:GCPProject{id: $PROJECT_ID}) SET p.lastupdated = $UPDATE_TAG",
            PROJECT_ID=GCP_PROJECT_ID,
            UPDATE_TAG=update_tag,
        )

        # STEP 2: Run the GCF sync.
        logger.info("Starting GCF sync for project '%s'...", GCP_PROJECT_ID)
        common_job_parameters = {
            "UPDATE_TAG": update_tag,
            "projectId": GCP_PROJECT_ID # Use camelCase for consistency
        }
        gcf.sync(
            session,
            functions_client,
            GCP_PROJECT_ID,
            update_tag,
            common_job_parameters,
        )
    logger.info("GCF sync complete.")

if __name__ == "__main__":
    main()