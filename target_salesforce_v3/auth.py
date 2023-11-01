import json
from datetime import datetime
from typing import Any, Dict, Mapping, Optional
from types import MappingProxyType
import logging

import requests

class SalesforceV3Authenticator:
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.target_name: str = target.name
        self._config: Dict[str, Any] = dict(target.config)
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
        self._target = target
        self.update_access_token()
        self.instance_url = self._target.config["instance_url"]

    @property
    def config(self) -> Mapping[str, Any]:
        """Get stream or tap config."""
        return MappingProxyType(self._config)

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._target._config.get('access_token')}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
            "client_id": self._target._config["client_id"],
            "client_secret": self._target._config["client_secret"],
            "redirect_uri": self._target._config["redirect_uri"],
            "refresh_token": self._target._config["refresh_token"],
            "grant_type": "refresh_token",
        }

    def is_token_valid(self) -> bool:
        access_token = self._target._config.get("access_token")
        now = datetime.now().timestamp()
        issued_at = self._target._config.get("issued_at")
        if not access_token or not issued_at:
            return False

        time_since_issued = now - issued_at / 1000

        return time_since_issued < 7000

    @property
    def oauth_request_payload(self) -> dict:
        """Get request body.

        Returns:
            A plain (OAuth) or encrypted (JWT) request body.
        """
        return self.oauth_request_body

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `issued_at`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        if self.is_token_valid() and self._target._config.get("instance_url"):
            return
        auth_request_payload = self.oauth_request_payload
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_response = requests.post(
            self.auth_endpoint, headers=headers, data=auth_request_payload
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        issued_at = int(token_json["issued_at"])

        self._target._config["access_token"] = token_json["access_token"]
        self._target._config["issued_at"] = issued_at
        self._target._config["instance_url"] = token_json["instance_url"]
        with open(self._target.config_file, "w") as outfile:
            json.dump(self._target._config, outfile, indent=4)
