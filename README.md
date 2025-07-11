# target-salesforce-v3

`target-salesforce-v3` is a Singer target for SalesforceV3.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-salesforce-v3
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-salesforce-v3.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-salesforce-v3 --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-salesforce-v3 --about
```

#### Key Configuration Options

**`only_upsert_empty_fields`** (boolean or array, optional)

- **Description**: When enabled, this option ensures that only fields with empty/null values in Salesforce will be updated during upsert operations. This prevents overwriting existing data with empty values.
- **Usage**:
  - **Boolean**: Set to `true` to apply this behavior to all fields
  - **Array**: Provide a list of specific unified field names that should only be updated when they are empty in Salesforce
- **Supported field types** (when using array):
  - **Simple fields**: Direct field mappings like `"email"`, `"first_name"`, `"last_name"`
  - **Complex fields**: Special handling for `"addresses"` and `"phone_numbers"` which map to multiple Salesforce fields
  - **Custom fields**: Salesforce custom fields like `"Investor__c"`

**Example configurations:**

**Boolean usage (apply to all fields):**

```json
{
  "only_upsert_empty_fields": true
}
```

**Array usage (apply to specific fields):**

```json
{
  "only_upsert_empty_fields": [
    "email",
    "first_name",
    "last_name",
    "Investor__c",
    "addresses",
    "phone_numbers"
  ]
}
```

**How it works:**

- When set to `true`, all fields will only be updated if they are currently empty in Salesforce
- When using an array, only the specified fields will be protected from overwriting existing data
- When `"addresses"` is included, it excludes all address-related Salesforce fields from updates if they already have values
- When `"phone_numbers"` is included, it excludes all phone-related Salesforce fields from updates if they already have values
- For simple fields, it uses the unified-to-Salesforce field mapping to determine which Salesforce field to exclude

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-salesforce-v3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-salesforce-v3 --version
target-salesforce-v3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-salesforce-v3 --config /path/to/target-salesforce-v3-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `target-salesforce-v3` CLI interface directly using `poetry run`:

```bash
poetry run target-salesforce-v3 --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-salesforce-v3
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-salesforce-v3 --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-salesforce-v3
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
