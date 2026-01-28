# target-salesforce-v3 Configuration

This document describes all configuration options available for the `target-salesforce-v3` Singer target.

## Configuration Options

### Authentication

These options are required for authenticating with Salesforce using OAuth 2.0.

#### `client_id` (string, required)
The OAuth 2.0 client ID from your Salesforce Connected App.
- **Example**: `"1234567890ABCDEF"`

#### `client_secret` (string, required)
The OAuth 2.0 client secret from your Salesforce Connected App.
- **Example**: `"1234567890ABCDEF"`

#### `redirect_uri` (string, required)
The redirect URI configured in your Salesforce Connected App. This must match exactly with the URI registered in Salesforce.
- **Example**: `"https://your-app.com/callback"`

#### `refresh_token` (string, required)
The OAuth 2.0 refresh token obtained during the initial OAuth flow.
- **Example**: `"1234567890ABCDEF"`

#### `instance_url` (string, required)
The Salesforce instance URL.
- **Example**: `"https://yourinstance.salesforce.com"`

#### `access_token` (string, optional)
The OAuth 2.0 access token. This is typically auto-populated and refreshed automatically by the target.
- **Example**: `"1234567890ABCDEF"`

#### `issued_at` (number, optional)
The timestamp when the access token was issued (in milliseconds). This is auto-populated during token refresh.
- **Example**: `1640995200000`

#### `base_uri` (string, optional)
The base URI for Salesforce authentication. Use `"https://test.salesforce.com"` for sandbox environments.
- **Example**: `"https://login.salesforce.com"` or `"https://test.salesforce.com"`

#### `is_sandbox` (boolean, optional)
Whether the Salesforce instance is a sandbox. If `base_uri` is set to `"https://test.salesforce.com"`, this is automatically detected.
- **Example**: `false`

### API Configuration

#### `api_version` (string, optional)
The Salesforce API version to use. The version number should be provided without the "v" prefix.
- **Default**: `"55.0"`
- **Example**: `"55.0"` or `"58.0"`

#### `user_agent` (string, optional)
Custom user agent string to include in HTTP requests to Salesforce.
- **Example**: `"target-salesforce-v3/0.0.1"`

#### `quota_percent_total` (number, optional)
The percentage of Salesforce daily REST API quota usage at which the target should stop processing to avoid exceeding limits.
- **Default**: `80`
- **Example**: `80` (stops at 80% quota usage)

### Feature Flags

#### `create_custom_fields` (boolean, optional)
If enabled, the target will automatically create custom fields in Salesforce when they don't exist. Custom fields will be created as Text fields with a length of 100 characters.
- **Default**: `false`
- **Example**: `true`

#### `only_upsert_empty_fields` (boolean, optional)
If enabled, when updating existing records, only fields that are currently empty/null will be updated. This prevents overwriting existing data.
- **Default**: `false`
- **Example**: `true`

#### `lookup_by_email` (boolean, optional)
If enabled, the fallback sink will attempt to find existing records by searching for email fields when no explicit lookup field is configured.
- **Default**: `true`
- **Example**: `true`

#### `only_upsert_accounts` (boolean, optional)
If enabled, the target will only update existing Account records and will not create new ones. This is useful when you want to prevent accidental account creation.
- **Default**: `false`
- **Example**: `true`

#### `lookup_fields` (object, optional)
A dictionary mapping Salesforce object types to field names that should be used for looking up existing records. This is primarily used by the fallback sink.
- **Example**: 
  ```json
  {
    "CustomObject__c": "ExternalId__c",
    "Account": "Custom_External_Id__c"
  }
  ```

## Configuration Examples

### Minimal Configuration

This example shows the minimum required configuration options:

```json
{
  "client_id": "your_client_id_here",
  "client_secret": "your_client_secret_here",
  "redirect_uri": "https://your-redirect-uri.com/callback",
  "refresh_token": "your_refresh_token_here",
  "instance_url": "https://yourinstance.salesforce.com"
}
```

### Complete Configuration

This example shows a more complete configuration with all available options:

```json
{
  "client_id": "your_client_id_here",
  "client_secret": "your_client_secret_here",
  "redirect_uri": "https://your-redirect-uri.com/callback",
  "refresh_token": "your_refresh_token_here",
  "instance_url": "https://yourinstance.salesforce.com",
  "api_version": "55.0",
  "user_agent": "target-salesforce-v3/0.0.1",
  "quota_percent_total": 80,
  "create_custom_fields": false,
  "only_upsert_empty_fields": false,
  "lookup_by_email": true,
  "only_upsert_accounts": false,
  "lookup_fields": {
    "CustomObject__c": "ExternalId__c"
  },
  "base_uri": "https://login.salesforce.com",
  "is_sandbox": false
}
```
