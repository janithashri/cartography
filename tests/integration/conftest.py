import neo4j
import pytest
from dynaconf import Dynaconf

# This loads settings from your config.yaml file.
settings = Dynaconf(
    settings_files=['config.yaml'],
)

@pytest.fixture(scope="function")
def neo4j_session():
    """
    Creates a Neo4j session for a test and cleans the database after the test runs.
    """
    # This reads the user and password from your config.yaml and sets up authentication.
    auth = (settings.get("NEO4J_USER"), settings.get("NEO4J_SECRET"))
    driver = neo4j.GraphDatabase.driver(settings.get("NEO4J_URI"), auth=auth)

    with driver.session() as session:
        yield session
        # This runs after each test to ensure the database is clean for the next one.
        session.run("MATCH (n) DETACH DELETE n;")