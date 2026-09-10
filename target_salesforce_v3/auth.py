from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator


def get_token_url(config: dict) -> str:
    """Build the Salesforce OAuth2 token endpoint."""
    if config.get("base_uri"):
        is_sandbox = config["base_uri"] == "https://test.salesforce.com"
    else:
        val = config.get("is_sandbox")
        is_sandbox = val is True or (isinstance(val, str) and val.lower() == "true")

    if is_sandbox:
        return "https://test.salesforce.com/services/oauth2/token"
    return "https://login.salesforce.com/services/oauth2/token"


class SalesforceV3Authenticator(OAuthAuthenticator):
    """OAuth authenticator for Salesforce."""

    def invalidate(self) -> None:
        """Force a token refresh on the next auth_headers access."""
        self.last_refreshed = None
        self.expires_in = None
        self._config["expires_in"] = None
