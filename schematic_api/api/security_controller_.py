import logging
from typing import Dict, Union

from jwt import PyJWKClient, decode
from jwt.exceptions import InvalidTokenError
from synapseclient import Synapse

from schematic.configuration.configuration import CONFIG

logger = logging.getLogger(__name__)

syn = Synapse(
    configPath=CONFIG.synapse_configuration_path,
    cache_client=False,
)
jwks_client = PyJWKClient(
    uri=syn.authEndpoint + "/oauth2/jwks", headers=syn._generate_headers()
)


def info_from_bearerAuth(token: str) -> Dict[str, Union[str, int]]:
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
        jwks_client.get_signing_key_from_jwt(token)
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        data = decode(
            jwt=token,
            key=signing_key.key,
            algorithms=["RS256"],
            options={"verify_aud": False},
        )

        return data
    except InvalidTokenError:
        logger.exception("Error decoding authentication token")
        # When the return type is None the web framework will return a 401 OAuthResponseProblem exception
        return None
