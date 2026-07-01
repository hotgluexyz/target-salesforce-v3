from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from hotglue_etl_exceptions import InvalidPayloadError

from target_salesforce_v3.sinks import FallbackSink


def test_fallback_sink_raises_invalid_payload_error_for_account_create_failure():
    sink = FallbackSink.__new__(FallbackSink)
    sink.logger = Mock()
    sink.stream_name = "Account"
    sink.name = "Account"
    sink.config = {}
    sink.key_properties = []
    sink._target = SimpleNamespace(read_only_fields={})
    sink.sf_fields_description = Mock(return_value={
        "createable": ["Name"],
        "custom": [],
        "required": [],
        "external_ids": [],
        "pickable": {},
    })
    sink.request_api = Mock(side_effect=Exception("INVALID_FIELD_FOR_INSERT_UPDATE: Name"))
    sink.link_attachment_to_object = Mock()
    sink._handle_person_account = Mock()

    with pytest.raises(
        InvalidPayloadError,
        match="Attempted to write read-only fields. Unable to extract read-only fields to retry request: INVALID_FIELD_FOR_INSERT_UPDATE: Name",
    ):
        sink.upsert_record({"object_type": "Account", "Name": "Invalid"}, context={})
