import math
from typing import Dict, Union

import pendulum
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.sources.helpers.rest_client.auth import OAuthJWTAuth


class GitHubAppAuth(OAuthJWTAuth):
    def create_jwt_payload(self) -> Dict[str, Union[str, int]]:
        now = pendulum.now()
        return {
            "iss": self.client_id,
            "exp": math.floor((now.add(minutes=10)).timestamp()),
            "iat": math.floor(now.timestamp()),
        }

    def obtain_token(self) -> None:
        try:
            import jwt
        except ModuleNotFoundError:
            raise MissingDependencyException("dlt OAuth helpers", ["PyJWT"])

        payload = self.create_jwt_payload()
        obtain_token_headers = {
            "Authorization": f"Bearer {jwt.encode(payload, self.load_private_key(), algorithm='RS256')}"
        }

        logger.debug(f"Obtaining token from {self.auth_endpoint}")

        response = self.session.post(self.auth_endpoint, headers=obtain_token_headers)
        response.raise_for_status()

        token_response = response.json()
        self.token = token_response["token"]
        self.token_expiry = pendulum.parse(token_response.get("expires_at"))
