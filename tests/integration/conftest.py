import neo4j
import pytest
from dynaconf import Dynaconf

# This explicitly loads your config.yaml file for the test run.
settings = Dynaconf(
    settings_files=['config.yaml'],
)

@pytest.fixture(scope="function")
def neo4j_session():
    # This `auth` tuple is required to connect to a password-protected database
    auth = (settings.get("NEO4J_USER"), settings.get("NEO4J_SECRET"))
    driver = neo4j.GraphDatabase.driver(settings.get("NEO4J_URI"), auth=auth)

    with driver.session() as session:
        yield session
        # This runs after each test to ensure the database is clean for the next test
        session.run("MATCH (n) DETACH DELETE n;")