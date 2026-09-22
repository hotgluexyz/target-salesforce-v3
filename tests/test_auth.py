from target_salesforce_v3.auth import SalesforceV3Authenticator, get_token_url


def test_get_token_url_login_by_default():
    assert get_token_url({}) == "https://login.salesforce.com/services/oauth2/token"


def test_get_token_url_sandbox_from_base_uri():
    assert (
        get_token_url({"base_uri": "https://test.salesforce.com"})
        == "https://test.salesforce.com/services/oauth2/token"
    )


def test_get_token_url_sandbox_from_flag():
    assert (
        get_token_url({"is_sandbox": True})
        == "https://test.salesforce.com/services/oauth2/token"
    )


def test_invalidate_clears_refresh_state():
    class _Target:
        name = "target-salesforce-v3"
        _config = {"expires_in": 999}
        _config_file_path = None
        logger = None

    auth = SalesforceV3Authenticator(
        _Target(),
        auth_endpoint="https://login.salesforce.com/services/oauth2/token",
    )
    auth.last_refreshed = object()
    auth.expires_in = 999
    auth.invalidate()

    assert auth.last_refreshed is None
    assert auth.expires_in is None
    assert auth._config["expires_in"] is None
