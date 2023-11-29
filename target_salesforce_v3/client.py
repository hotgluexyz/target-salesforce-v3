"""SalesforceV3 target sink class, which handles writing streams."""

from __future__ import annotations

import re

import backoff
import requests

from backports.cached_property import cached_property
from datetime import datetime

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.sinks import RecordSink

from target_salesforce_v3.auth import SalesforceV3Authenticator

from target_hotglue.sinks import HotglueSink


class TargetSalesforceQuotaExceededException(Exception):
    pass


class MissingRequiredFieldException(Exception):
    pass


class NoCreatableFieldsException(Exception):
    pass


class SalesforceV3Sink(HotglueSink, RecordSink):
    """SalesforceV3 target sink class."""
    api_version = "v55.0"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        headers.update(self.authenticator.auth_headers or {})
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {self.endpoint}"
        )

    def check_salesforce_limits(self, response):
        limit_info = response.headers.get("Sforce-Limit-Info")
        quota_percent_total = 80

        match = re.search("^api-usage=(\d+)/(\d+)$", limit_info)
        if match is None:
            return
        remaining, allotted = map(int, match.groups())

        self.logger.info("Used %s of %s daily REST API quota", remaining, allotted)
        percent_used_from_total = (remaining / allotted) * 100

        if percent_used_from_total > quota_percent_total:
            total_message = (
                "Salesforce has reported {}/{} ({:3.2f}%) total REST quota "
                "used across all Salesforce Applications. Terminating "
                "replication to not continue past configured percentage "
                "of {}% total quota."
            ).format(remaining, allotted, percent_used_from_total, quota_percent_total)
            raise TargetSalesforceQuotaExceededException(total_message)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )
        self.validate_response(response)
        return response

    def request_api(self, http_method, endpoint=None, params=None, request_data=None, headers=None):
        """Request records from REST endpoint(s), returning response records."""
        resp = self._request(http_method, endpoint, params, request_data, headers)
        self.check_salesforce_limits(resp)
        return resp

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        # Getting custom fields from record
        # self.process_custom_fields(record)

        fields = self.sf_fields_description()

        for field in fields["external_ids"]:
            if record.get(field):
                try:
                    update_record = record.copy()
                    update_record.pop(field)
                    url = "/".join([self.endpoint, field, record[field]])
                    response = self.request_api(
                        "PATCH", endpoint=url, request_data=update_record
                    )
                    id = response.json().get("id")
                    self.logger.info(f"{self.name} updated with id: {id}")
                    return id, True, state_updates
                except:
                    self.logger.info(f"{field} with id {record[field]} does not exist.")

        if "Id" in record:
            if "ContactId" in record.keys():
                del record["ContactId"]
            id = record.pop("Id")
            url = "/".join([self.endpoint, id])
            response = self.request_api("PATCH", endpoint=url, request_data=record)
            response.raise_for_status()
            self.logger.info(f"{self.name} updated with id: {id}")
            return id, True, state_updates

        response = self.request_api("POST", request_data=record)
        try:
            id = response.json().get("id")
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates
        except:
            pass
    
    @property
    def authenticator(self):
        url = self.url()
        return SalesforceV3Authenticator(
            self._target,
            url
        )

    @staticmethod
    def clean_dict_items(dict):
        return {k: v for k, v in dict.items() if v not in [None, ""]}

    def clean_payload(self, item):
        item = self.clean_dict_items(item)
        output = {}
        for k, v in item.items():
            if isinstance(v, datetime):
                dt_str = v.strftime("%Y-%m-%dT%H:%M:%S%z")
                if len(dt_str) > 20:
                    output[k] = f"{dt_str[:-2]}:{dt_str[-2:]}"
                else:
                    output[k] = dt_str
            elif isinstance(v, dict):
                output[k] = self.clean_payload(v)
            else:
                output[k] = v
        return output

    def url(self, endpoint=None):
        if not endpoint:
            endpoint = self.endpoint
        instance_url = self.config.get("instance_url")
        if not instance_url:
            raise Exception("instance_url not defined in config")
        return f"{instance_url}/services/data/{self.api_version}/{endpoint}"

    def validate_input(self, record: dict):
        return self.unified_schema(**record).dict()

    def sf_fields(self, object_type=None):
        if not object_type:
            sobject = self.request_api("GET", f"{self.endpoint}/describe/")
        else:
            sobject = self.request_api("GET", f"sobjects/{object_type}/describe/")
        return [f for f in sobject.json()["fields"]]

    def sf_fields_description(self, object_type=None):
        fld = self.sf_fields(object_type=object_type)
        fields = {}
        fields["createable"] = [
            f["name"] for f in fld if f["createable"] and not f["custom"]
        ]
        fields["custom"] = [
            f["name"] for f in fld if f["custom"]
        ]
        fields["createable_not_default"] = [
            f["name"]
            for f in fld
            if f["createable"] and not f["defaultedOnCreate"] and not f["custom"]
        ]
        fields["required"] = [
            f["name"]
            for f in fld
            if not f["nillable"] and f["createable"] and not f["defaultedOnCreate"]
        ]
        fields["external_ids"] = [f["name"] for f in fld if f["externalId"]]
        fields["pickable"] = {}
        for field in fld:
            if field["picklistValues"]:
                fields["pickable"][field["name"]] = [
                    p["label"] for p in field["picklistValues"] if p["active"]
                ]
        return fields

    def get_pickable(self, record_field, sf_field, default=None, select_first=False):
        fields_dict = self.sf_fields_description()
        pickable_fields = fields_dict["pickable"]
        if sf_field not in pickable_fields:
            return default
        valid_options = [re.sub(r'\W+', '', choice).lower() for choice in pickable_fields[sf_field]]
        nice_valid_options = [choice for choice in pickable_fields[sf_field]]

        if record_field not in valid_options:
            if select_first:
                self.logger.warning(
                    f"Using {nice_valid_options[0]} as {sf_field} {record_field} is not valid, valid values are {nice_valid_options}"
                )
                record_field = valid_options[0]
            else:
                record_field = default
        else:
            record_field = nice_valid_options[valid_options.index(record_field)]
        return record_field

    def sf_field_detais(self, field_name):
        fields = self.sf_fields
        return next((f for f in fields if f["name"] == field_name), None)

    def validate_output(self, mapping):
        mapping = self.clean_payload(mapping)
        payload = {}
        fields_dict = self.sf_fields_description()
        if not fields_dict["createable"]:
            raise NoCreatableFieldsException(f"No creatable fields for stream {self.name} stream, check your permissions")
        for k, v in mapping.items():
            if k.endswith("__c") or k in fields_dict["createable"] + ["Id"]:
                payload[k] = v

        # required = self.sf_fields_description["required"]
        # for req_field in required:
        #     if req_field not in payload:
        #         raise MissingRequiredFieldException(req_field)
        return payload

    def query_sobject(self, query, fields):
        params = {"q": query}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in fields} for r in response]

    def process_custom_fields(self, record) -> None:
        """
            Process the custom fields for Salesforce,
            creating unexsisting custom fields based on the present custom fields available in the record.

            Inputs:
            - record
        """

        # If the config.json does not specify to create the custom fields
        # automatically, then just don't execute this function
        if not self.config.get('create_custom_fields', False):
            return None

        # Checking if the custom fields already exist in
        fields_dict = self.sf_fields_description()
        salesforce_custom_fields = fields_dict['custom']

        for cf in record:
            cf_name = cf['name']
            if not cf_name.endswith('__c'):
                cf_name+='__c'
            if cf_name not in salesforce_custom_fields:
                # If there's a custom field in the record that is not in Salesforce
                # create it
                self.add_custom_field(cf['name'],label = cf.get('label'))

        return None

    def add_custom_field(self,cf,label=None):
        if not label:
            label = cf

        if not cf.endswith('__c'):
            cf += '__c'
        # Getting token and building the payload
        access_token = self.http_headers['Authorization'].replace('Bearer ','')
        sobject = self.endpoint.replace('sobjects/','')

        if sobject == 'Task':
            # If it's a task's custom field we need to create it under
            # `Activity` sObject, so we change `Task` -> `Activity`
            sobject = 'Activity'

        url = self.url("services/Soap/m/55.0").replace('services/data/v55.0/','')

        # If the new custom field is an external id it needs to contain 'externalid'
        external_id = 'true' if 'externalid' in cf.lower() else 'false'

        xml_payload = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
                            <s:Header>
                                <h:SessionHeader xmlns:h="http://soap.sforce.com/2006/04/metadata"
                                xmlns="http://soap.sforce.com/2006/04/metadata"
                                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                xmlns:xsd="http://www.w3.org/2001/XMLSchema">
                                <sessionId>{access_token}</sessionId>
                                </h:SessionHeader>
                            </s:Header>
                            <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                xmlns:xsd="http://www.w3.org/2001/XMLSchema">
                                <createMetadata xmlns="http://soap.sforce.com/2006/04/metadata">
                                <metadata xsi:type="CustomField">
                                    <fullName>{sobject}.{cf}</fullName>
                                    <label>{label}</label>
                                    <externalId>{external_id}</externalId>
                                    <type>Text</type>
                                    <length>100</length>
                                </metadata>
                                </createMetadata>
                            </s:Body>
                        </s:Envelope>"""

        response = requests.request(
            method="POST",
            url=url,
            headers={'Content-Type':"text/xml","SOAPAction":'""'},
            data=xml_payload
        )
        self.validate_response(response)

        # update field permissions for custom field per profile
        if sobject == 'Activity':
            # But then, we need to add the permissions to the Task sObject
            # So we change it back again from `Activity` -> `Task`
            sobject = 'Task'
        for permission_set_id in self.permission_set_ids:
            self.update_field_permissions(permission_set_id, sobject_type=sobject, field_name=f"{sobject}.{cf}")

    def update_field_permissions(self,permission_set_id, sobject_type, field_name):
        payload = {
            "allOrNone": True,
            "compositeRequest": [
                {
                    "referenceId": "NewFieldPermission",
                    "body": {
                        "ParentId": permission_set_id,
                        "SobjectType": sobject_type,
                        "Field": field_name,
                        "PermissionsEdit": "true",
                        "PermissionsRead": "true"
                    },
                    "url": "/services/data/v55.0/sobjects/FieldPermissions/",
                    "method": "POST"
                }
            ]
        }

        response = self.request_api("POST", endpoint="composite", request_data=payload, headers={"Content-Type": "application/json"})
