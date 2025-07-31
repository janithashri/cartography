import logging
import json
from typing import Optional
from google.auth import default
from google.auth.credentials import Credentials as GoogleCredentials
from google.auth.exceptions import DefaultCredentialsError

from cartography.util import timeit # Assuming timeit is in cartography.util

logger = logging.getLogger(__name__)

@timeit
def get_gcp_credentials() -> Optional[GoogleCredentials]:
    """
    Gets access tokens for GCP API access.
    This function is moved here to break a circular dependency.
    :return: GoogleCredentials object or None if authentication fails.
    """
    try:
        # Explicitly use Application Default Credentials.
        # See https://google-auth.readthedocs.io/en/master/user-guide.html#application-default-credentials
        credentials, project_id = default()
        return credentials
    except DefaultCredentialsError as e:
        logger.debug(
            "Error occurred calling GoogleCredentials.get_application_default().",
            exc_info=True,
        )
        logger.error(
            (
                "Unable to initialize GCP credentials. If you don't have GCP data or don't want to load "
                "GCP data then you can ignore this message. Otherwise, the error code is: %s "
                "Make sure your GCP credentials are configured correctly, your credentials file (if any) is valid, and "
                "that the identity you are authenticating to has the securityReviewer role attached."
            ),
            e,
        )
    return None