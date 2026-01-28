import json
from datetime import datetime
from typing import Any, Dict, Mapping
from types import MappingProxyType
import logging

import requests
from hotglue_etl_exceptions import InvalidCredentialsError

class SalesforceV3Authenticator:
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        auth_endpoint: str,
    ) -> None:
        """Initialize authenticator.

        Args:
            target: The target instance for Salesforce API authentication.
            auth_endpoint: Authorization endpoint URL.
        """
        self._config: Dict[str, Any] = dict(target.config)
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
        self._target = target

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

        is_sandbox = (
            self._target._config.get("base_uri") == "https://test.salesforce.com"
            if self._target._config.get("base_uri")
            else self._target._config.get("is_sandbox")
        )

        if is_sandbox:
            login_url = 'https://test.salesforce.com/services/oauth2/token'
        else:
            login_url = 'https://login.salesforce.com/services/oauth2/token'

        token_response = requests.post(
            login_url,
            headers=headers,
            data=auth_request_payload
        )
        error_codes = ["invalid_grant", "invalid_client"]
        if token_response.status_code == 400 and any(error_code in token_response.text for error_code in error_codes):
            try:
                msg = token_response.json()["error_description"]
            except:
                msg = token_response.text
            raise InvalidCredentialsError(msg)
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
        with open(self._target._config_file_path, "w") as outfile:
            json.dump(self._target._config, outfile, indent=4)
