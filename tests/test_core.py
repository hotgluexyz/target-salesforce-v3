"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from hotglue_singer_sdk.testing import get_standard_target_tests

from target_salesforce_v3.target import TargetSalesforceV3

SAMPLE_CONFIG: dict[str, t.Any] = {}

standard_target_tests = get_standard_target_tests(
    target_class=TargetSalesforceV3,
    config=SAMPLE_CONFIG,
)


class TestTargetSalesforceV3:
    """Standard Target Tests."""

    @pytest.mark.parametrize(
        "test_fn",
        standard_target_tests,
        ids=[fn.__name__ for fn in standard_target_tests],
    )
    def test_standard(self, test_fn):
        test_fn()
