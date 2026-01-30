"""SalesforceV3 target class."""

from __future__ import annotations

from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from target_salesforce_v3.sinks import (
    FallbackSink,
    ContactsSink,
    DealsSink,
    CompanySink,
    RecurringDonationsSink,
    CampaignSink,
    CampaignMemberSink,
    ActivitiesSink,
)


SINK_TYPES = [
    ContactsSink,
    DealsSink,
    CompanySink,
    RecurringDonationsSink,
    CampaignSink,
    CampaignMemberSink,
    ActivitiesSink,
]


class TargetSalesforceV3(TargetHotglue):
    """Sample target for Api."""

    name = "target-salesforce-v3"
    alerting_level = AlertingLevel.WARNING
    MAX_PARALLELISM = 10
    SINK_TYPES = SINK_TYPES
    read_only_fields = {}
    GLOBAL_PRIMARY_KEY = "Id"

    def get_sink_class(self, stream_name: str):
        """Get sink for a stream."""
        for sink_class in SINK_TYPES:
            if sink_class.name.lower() == stream_name.lower():
                return sink_class

            # Search for streams with multiple names
            if stream_name.lower() in sink_class.available_names:
                return sink_class

        # Adds a fallback sink for streams that are not supported
        return FallbackSink


if __name__ == "__main__":
    TargetSalesforceV3.cli()
