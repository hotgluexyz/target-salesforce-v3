"""SalesforceV3 target class."""

from __future__ import annotations

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

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
    MAX_PARALLELISM = 10
    SINK_TYPES = SINK_TYPES
    def get_sink_class(self, stream_name: str):
        """Get sink for a stream."""
        for sink_class in SINK_TYPES:
            if sink_class.name.lower() == stream_name.lower():
                return sink_class

            # Search for streams with multiple names
            if stream_name.lower() in sink_class.available_names: #[name.lower() for name in sink_class.available_names]:
                return sink_class

        # Adds a fallback sink for streams that are not supported
        return FallbackSink


if __name__ == "__main__":
    TargetSalesforceV3.cli()
