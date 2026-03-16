class TestStripEmptyAssociations:
    """Unit tests for _strip_empty_associations."""

    @staticmethod
    def _strip(data):
        from target_salesforce_v3.client import SalesforceV3Sink
        return SalesforceV3Sink._strip_empty_associations(data)

    def test_removes_association_with_empty_string_value(self):
        data = {"Name": "Test", "Beneficiary__r": {"Sponsorship_ID__c": ""}}
        assert self._strip(data) == {"Name": "Test"}

    def test_removes_association_with_none_value(self):
        data = {"Name": "Test", "Beneficiary__r": {"Sponsorship_ID__c": None}}
        assert self._strip(data) == {"Name": "Test"}

    def test_removes_association_with_empty_dict(self):
        data = {"Name": "Test", "Beneficiary__r": {}}
        assert self._strip(data) == {"Name": "Test"}

    def test_keeps_association_with_valid_value(self):
        data = {"Name": "Test", "Beneficiary__r": {"Sponsorship_ID__c": "SP-123"}}
        assert self._strip(data) == data

    def test_keeps_association_if_any_value_is_populated(self):
        data = {"Beneficiary__r": {"Id__c": "123", "Other__c": ""}}
        assert self._strip(data) == data

    def test_keeps_non_association_fields_unchanged(self):
        data = {"Name": "", "Email": None, "Custom__c": "val"}
        assert self._strip(data) == data

    def test_handles_non_dict_input(self):
        assert self._strip("not a dict") == "not a dict"
        assert self._strip(None) is None

    def test_multiple_associations_mixed(self):
        data = {
            "Name": "Test",
            "Good__r": {"ExternalId__c": "abc"},
            "Bad__r": {"ExternalId__c": ""},
            "AlsoBad__r": {"Id__c": None, "Other__c": ""},
        }
        expected = {
            "Name": "Test",
            "Good__r": {"ExternalId__c": "abc"},
        }
        assert self._strip(data) == expected
