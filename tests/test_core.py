"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from hotglue_singer_sdk.testing import get_target_test_class

from target_salesforce_v3.target import TargetSalesforceV3

# TODO: Initialize minimal target config
SAMPLE_CONFIG: dict[str, t.Any] = {}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetSalesforceV3,
    config=SAMPLE_CONFIG,
)


class TestTargetSalesforceV3(StandardTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"


# TODO: Create additional tests as appropriate for your target.
