import logging
from typing import Dict, Union

from jwt import PyJWKClient, decode
from jwt.exceptions import PyJWTError
from synapseclient import Synapse  # type: ignore

from schematic.configuration.configuration import CONFIG

logger = logging.getLogger(__name__)

syn = Synapse(
    configPath=CONFIG.synapse_configuration_path,
    cache_client=False,
    skip_checks=True,
)
jwks_client = PyJWKClient(
    uri=syn.authEndpoint + "/oauth2/jwks", headers=syn._generate_headers()
)


def info_from_bearer_auth(token: str) -> Union[Dict[str, Union[str, int]], None]:
    """
    Authenticate user using bearer token. The token claims are decoded and returned.

    Example from:
    <https://pyjwt.readthedocs.io/en/stable/usage.html#retrieve-rsa-signing-keys-from-a-jwks-endpoint>

    Args:
        token (str): Bearer token.

    Returns:
        dict: Decoded token information.
    """
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        data = decode(
            jwt=token,
            key=signing_key.key,
            algorithms=[signing_key.algorithm_name],
            options={"verify_aud": False},
        )

        return data
    except PyJWTError:
        logger.exception("Error decoding authentication token")
        # When the return type is None the web framework will return a 401 OAuthResponseProblem exception
        return None
