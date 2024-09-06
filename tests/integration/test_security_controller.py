import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from pytest import LogCaptureFixture

from schematic_api.api.security_controller import info_from_bearer_auth


class TestSecurityController:
    def test_valid_synapse_token(self, syn_token: str) -> None:
        # GIVEN a valid synapse token
        assert syn_token is not None

        # WHEN the token is decoded
        decoded_token = info_from_bearer_auth(syn_token)

        # THEN the decoded claims are a dictionary
        assert isinstance(decoded_token, dict)
        assert "sub" in decoded_token
        assert decoded_token["sub"] is not None
        assert "token_type" in decoded_token
        assert decoded_token["token_type"] is not None

    def test_invalid_synapse_signing_key(self, caplog: LogCaptureFixture) -> None:
        # GIVEN an invalid synapse token
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

        random_token = jwt.encode(
            payload={"sub": "random"}, key=private_key, algorithm="RS256"
        )

        # WHEN the token is decoded
        decoded_token = info_from_bearer_auth(random_token)

        # THEN nothing is returned
        assert decoded_token is None

        # AND an error is logged
        assert (
            "jwt.exceptions.PyJWKClientError: Unable to find a signing key that matches:"
            in caplog.text
        )

    def test_invalid_synapse_token_not_enough_parts(
        self, caplog: LogCaptureFixture
    ) -> None:
        # GIVEN an invalid synapse token
        random_token = "invalid token"

        # WHEN the token is decoded
        decoded_token = info_from_bearer_auth(random_token)

        # THEN nothing is returned
        assert decoded_token is None

        # AND an error is logged
        assert "jwt.exceptions.DecodeError: Not enough segments" in caplog.text
