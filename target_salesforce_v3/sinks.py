"""SalesforceV3 target sink class, which handles writing streams."""
from __future__ import annotations

import json

from target_salesforce_v3.client import SalesforceV3Sink

from hotglue_models_crm.crm import Contact, Company, Deal, Campaign,Activity
from backports.cached_property import cached_property

from dateutil.parser import parse
from datetime import datetime
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class MissingObjectInSalesforceError(Exception):
    pass


class ContactsSink(SalesforceV3Sink):
    endpoint = "sobjects/Contact"
    unified_schema = Contact
    name = Contact.Stream.name
    campaigns = None
    contact_type = "Contact"
    available_names = ["contacts", "customers"]

    @cached_property
    def reference_data(self):
        params = {"q": "SELECT id, name from Account"}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]

    def preprocess_record(self, record, context):

        if isinstance(record.get("addresses"), str):
            record["addresses"] = json.loads(record["addresses"])

        if isinstance(record.get("phone_numbers"), str):
            record["phone_numbers"] = json.loads(record.get("phone_numbers"))

        if isinstance(record.get("campaigns"), str):
            record["campaigns"] = json.loads(record.get("campaigns"))

        record = self.validate_input(record)

        # Handles creation/update of Leads and Contacts
        if record.get("type") == "lead":
            self.contact_type = "Lead"
            self.endpoint = "sobjects/Lead"
        else:
            self.contact_type = "Contact"
            self.endpoint = "sobjects/Contact"

        lead_source = self.get_pickable(record.get("lead_source"), "LeadSource")
        salutation = self.get_pickable(record.get("salutation"), "Salutation")
        industry = self.get_pickable(record.get("industry"), "Industry")
        rating = self.get_pickable(record.get("rating"), "Rating")

        birthdate = record.get("birthdate")
        if birthdate is not None:
            birthdate = birthdate.strftime("%Y-%m-%d")

        # fields = self.sf_fields_description

        mapping = {
            "FirstName": record.get("first_name"),
            "LastName": record.get("last_name"),
            "Email": record.get("email"),
            "Title": record.get("title"),
            "Description": record.get("description"),
            "LeadSource": lead_source,
            "Salutation": salutation,
            "Birthdate": birthdate,
            "OwnerId": record.get("owner_id"),
            "HasOptedOutOfEmail": record.get("unsubscribed"),
            "NumberOfEmployees": record.get("number_of_employees"),
            "Website": record.get("website"),
            "Industry": industry,
            "Company": record.get("company_name"),
            "Rating": rating,
            "AnnualRevenue": record.get("annual_revenue"),
        }

        mapping_copy = mapping.copy()
        for key,value in mapping_copy.items():
            if value is None: mapping.pop(key)
        del mapping_copy

        if self.contact_type == "Contact":
            mapping.update({"Department": record.get("department")})
        elif self.contact_type == "Lead":
            mapping.update({"Company": record.get("company_name")})

        if record.get('id'):
            # If contact has an Id will use it to updatev
            mapping.update({"Id": record['id']})
        elif record.get("external_id"):
            external_id = record["external_id"]
            mapping[external_id["name"]] = external_id["value"]
        else:
            # If no Id we'll use email to search for an existing record
            if record.get('email'):
                # Get contact_id based on email
                data = self.query_sobject(
                    query = f"SELECT Name, Id from {self.contact_type} WHERE Email = '{record.get('email')}'",
                    fields = ['Name', 'Id']
                )
                if data:
                    mapping.update({"Id":data[0].get("Id")})

        if record.get('campaigns'):
            self.campaigns = record['campaigns']
        else:
            self.campaigns = None

        if record.get("addresses"):
            address = record["addresses"][0]
            street = " - ".join(
                [v for k, v in address.items() if "line" in k and v is not None]
            )
            if self.contact_type == "Contact":
                _prefix = "Mailing"
            else: _prefix = ""

            mapping[f"{_prefix}Street"] = street
            mapping[f"{_prefix}City"] = address.get("city")
            mapping[f"{_prefix}State"] = address.get("state")
            mapping[f"{_prefix}PostalCode"] = address.get("postal_code")
            mapping[f"{_prefix}Country"] = address.get("country")

        if record.get("addresses") and len(record["addresses"]) >= 2 and self.contact_type == 'Contact':
            # Leads only have one address
            address = record["addresses"][1]
            street = " - ".join(
                [v for k, v in address.items() if "line" in k and v is not None]
            )
            mapping["OtherStreet"] = street
            mapping["OtherCity"] = address.get("city")
            mapping["OtherState"] = address.get("state")
            mapping["OtherPostalCode"] = address.get("postal_code")
            mapping["OtherCountry"] = address.get("country")

        phone_types = {
            "Phone": ["primary"],
            "OtherPhone": ["secondary"],
            "MobilePhone": ["mobile"],
            "HomePhone": ["home"],
        }

        phones = record.get("phone_numbers") or []
        for i, phone in enumerate(phones):
            type = phone.get("type")
            phone_type = list(phone_types.keys())[i]
            phone_type = next(
                (p for p, t in phone_types.items() if type in t), phone_type
            )
            mapping[phone_type] = phone.get("number")

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        if not mapping.get("AccountId") and record.get("company_name"):
            account_id = (
                r["Id"]
                for r in self.reference_data
                if r["Name"] == record["company_name"]
            )
            mapping["AccountId"] = next(account_id, None)

        return self.validate_output(mapping)

    def process_record(self, record, context):
        """Process the record."""

        # Getting custom fields from record
        # self.process_custom_fields(record)

        if record.get("Id"):
            fields = ["Id"]
        else:
            fields_dict = self.sf_fields_description()
            fields = fields_dict["external_ids"]

        for field in fields:
            if record.get(field):
                try:
                    update_record = record.copy()
                    id = update_record.pop(field)
                    if update_record:
                        url = "/".join([self.endpoint, field, record[field]])

                        response = self.request_api(
                            "PATCH", endpoint=url, request_data=update_record
                        )
                        id = response.json().get("id")
                    self.logger.info(f"{self.name} updated with id: {id}")
                    record = None

                    # Check for campaigns to be added
                    if self.campaigns:
                        self.assign_to_campaign(id,self.campaigns)
                    return
                except Exception as e:
                    self.logger.exception(f"Could not PATCH to {url}: {e}")
        if record:

            try:
                response = self.request_api("POST", request_data=record)
                id = response.json().get("id")
                self.logger.info(f"{self.contact_type} created with id: {id}")
                # Check for campaigns to be added
                if self.campaigns:
                    self.assign_to_campaign(id,self.campaigns)
            except Exception as e:
                self.logger.exception("Error while attempting to create Contact")
                raise e

    def validate_response(self, response):
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            if "Already a campaign member." in response.text:
                self.logger.info("INFO: This Contact/Lead is already a Campaign Member.")
            elif '[{"errorCode":"NOT_FOUND","message":"The requested resource does not exist"}]' in response.text:
                self.logger.info("INFO: This Contact/Lead was not found using Email will attempt to create it.")
            if '[{"message":"No such column \'HasOptedOutOfEmail\' on sobject of type' in response.text:
                self.update_field_permissions(profile = 'System Administrator', sobject_type = self.contact_type, field_name=f"{self.contact_type}.HasOptedOutOfEmail")
                raise RetriableAPIError(f"DEBUG: HasOptedOutOfEmail column was not found, updating 'Field-Leve Security'\n'System Administrator'[x]")
            else:
                try:
                    msg = response.text
                except:
                    msg = self.response_error_message(response)
                raise FatalAPIError(msg)

    def assign_to_campaign(self, contact_id, campaigns):
        """
        This function recieves a contact_id and a list of campaigns and assigns the contact_id to each campaign

        Input:
        contact_id : str
        campaigns : list[dict] eg. [{'id': None, 'name': 'Big Campaign'}, {'id': None, 'name': 'Huge Campaign'}]
        """

        for campaign in campaigns:

            # Checks if there's an id, if not, query it
            # Assuming campaigns are always created first
            if campaign.get("id") is None:
                # data = self.get_query(endpoint=f"sobjects/Campaign/Name/{campaign.get('name')}")
                data = self.query_sobject(
                    query = f"SELECT Id, CreatedDate from Campaign WHERE Name = '{campaign.get('name')}' ORDER BY CreatedDate ASC",
                    fields = ['Id']
                    )
                # Extract capaign id from record
                if not data:
                    self.logger.info(f"No Campaign found with Name = '{campaign.get('name')}'\nSkipping campaign ...")
                    continue
                campaign['campaign_id'] = data[0]['Id']

            # Assigns the customer_id to the campaign_id or lead_id
            mapping = {"CampaignId": campaign.get("campaign_id") or campaign.get("id")}
            if self.contact_type == "Contact":
                mapping.update({"ContactId": contact_id})
            else:
                mapping.update({"LeadId": contact_id})

            # Create the CampaignMember
            self.logger.info(f"INFO: Adding Contact/Lead Id:[{contact_id}] as a CampaignMember of Campaign Id:[{mapping.get('CampaignId')}].")

            try:
                response = self.request_api("POST",endpoint="sobjects/CampaignMember",request_data=mapping)

                id = response.json().get("id")
                self.logger.info(f"CampaignMember created with id: {id}")
                # Check for campaigns to be added
                if self.campaigns:
                    self.assign_to_campaign(id,self.campaigns)
            except Exception as e:
                self.logger.exception("Error encountered while creating CampaignMember")
                raise e


class DealsSink(SalesforceV3Sink):
    endpoint = "sobjects/Opportunity"
    unified_schema = Deal
    name = Deal.Stream.name
    available_names = ["deal", "opportunities", "deals"]

    @cached_property
    def reference_data(self):
        params = {"q": "SELECT id, name from Account"}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]

    def preprocess_record(self, record, context):
        if isinstance(record.get("custom_fields"), str):
            record["custom_fields"] = json.loads(record["custom_fields"])

        record = self.validate_input(record)

        stage = record.get("pipeline_stage_id")
        if not stage:
            stage = record.get("status") # fallback on field

        stage = self.get_pickable(stage, "StageName", select_first=True)

        type = record.get("type")
        type = self.get_pickable(type, "Type")

        if record.get("contact_external_id") and not record.get("contact_id"):
            external_id = record["contact_external_id"]
            url = "/".join(["sobjects/Contact", external_id["name"], external_id["value"]])
            response = self.request_api("GET", endpoint=url)
            record["contact_id"] = response.json().get("Id")
        else:
            # Tries to get contact_id and account_id from email
            data = self.query_sobject(
                query = f"SELECT Id, AccountId from Contact WHERE Email = '{record.get('contact_email')}'",
                fields = ['Id', 'AccountId']
            )
            if len(data) > 0:
                record["contact_id"] = data[0].get("Id")
                record["company_id"] = data[0].get("AccountId")

        mapping = {
            "Name": record.get("title"),
            "StageName": stage,
            "CloseDate": record["close_date"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "Description": record.get("description"),
            "Type": type,
            "Amount": record.get("monetary_amount"),
            "Probability": record.get("win_probability"),
            "LeadSource": record.get("lead_source"),
            "TotalOpportunityQuantity": record.get("expected_revenue"),
            "AccountId": record.get("company_id"),
            "OwnerId": record.get("owner_id"),
            "ContactId": record.get("contact_id"),
        }

        if not mapping.get("AccountId") and record.get("company_name"):
            account_id = (
                r["Id"]
                for r in self.reference_data
                if r["Name"] == record["company_name"]
            )
            mapping["AccountId"] = next(account_id, None)

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        if record.get("external_id"):
            external_id = record["external_id"]
            mapping[external_id["name"]] = external_id["value"]

        return self.validate_output(mapping)


class CompanySink(SalesforceV3Sink):
    endpoint = "sobjects/Account"
    unified_schema = Company
    name = Company.Stream.name
    available_names = ["company", "companies"]

    def preprocess_record(self, record, context):
        if isinstance(record.get("custom_fields"), str):
            record["custom_fields"] = json.loads(record["custom_fields"])

        if isinstance(record.get("addresses"), str):
            record["addresses"] = json.loads(record["addresses"])

        if isinstance(record.get("phone_numbers"), str):
            record["phone_numbers"] = json.loads(record.get("phone_numbers"))

        record = self.validate_input(record)

        type = "Customer - Direct"
        type = self.get_pickable(type, "Type")

        mapping = {
            "Name": record.get("name"),
            "Site": record.get("website"),
            "Type": type,
            "Industry": record.get("industry"),
            "Description": record.get("description"),
            "OwnerId": record.get("owner_id"),
        }

        if record.get("addresses"):
            address = record["addresses"][0]
            street = " - ".join(
                [v for k, v in address.items() if "line" in k and v is not None]
            )
            mapping["BillingStreet"] = street
            mapping["BillingCity"] = address.get("city")
            mapping["BillingState"] = address.get("state")
            mapping["BillingPostalCode"] = address.get("postal_code")
            mapping["BillingCountry"] = address.get("country")

        if record.get("addresses") and len(record["addresses"]) >= 2:
            address = record["addresses"][1]
            street = "\n".join([v for k, v in address if "line" in k and v is not None])
            mapping["ShippingStreet"] = street
            mapping["ShippingCity"] = address.get("city")
            mapping["ShippingState"] = address.get("state")
            mapping["ShippingPostalCode"] = address.get("postal_code")
            mapping["ShippingCountry"] = address.get("country")

        phone_types = {"Phone": ["primary"], "Fax": ["fax"]}

        phones = record.get("phone_numbers", [])
        for i, phone in enumerate(phones):
            type = phone.get("type")
            phone_type = list(phone_types.keys())[i]
            phone_type = next(
                (p for p, t in phone_types.items() if type in t), phone_type
            )
            mapping[phone_type] = phone.get("number")

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        return self.validate_output(mapping)


class RecurringDonationsSink(SalesforceV3Sink):
    endpoint = "sobjects/npe03__Recurring_Donation__c"
    name = "RecurringDonations"
    available_names = ["recurringdonations", "recurring_donations"]

    @cached_property
    def reference_accounts(self):
        params = {"q": "SELECT id, name from Account"}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]

    @cached_property
    def reference_contacts(self):
        params = {"q": "SELECT id, name from Contact"}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]

    def preprocess_record(self, record, context):

        installment_period = record.get("installment_period").title()
        installment_period = self.get_pickable(
            installment_period, "npe03__Installment_Period__c"
        )

        fields_dict = self.sf_fields_description()
        if record.get("created_at"):
            created_at = parse(record.get("created_at"))
        else:
            created_at = datetime.now()
        created_at = created_at.strftime("%Y-%m-%d")
        mapping = {
            "Name": record.get("name"),
            "npe03__Amount__c": record.get("amount"),
            "npe03__Installment_Period__c": installment_period,
            "npe03__Date_Established__c": created_at,
        }

        if not mapping.get("npe03__Contact__c") and record.get("contact_external_id"):
            contact_ext = record['contact_external_id']
            endpoint = f"sobjects/Contact/{contact_ext['name']}/{contact_ext['value']}"
            contact = self.request_api("GET", endpoint=endpoint)
            mapping["npe03__Contact__c"] = contact.json()["Id"]

        elif not mapping.get("npe03__Organization__c") and record.get("company_name"):
            account_id = (
                r["Id"]
                for r in self.reference_accounts
                if r["Name"] == record["company_name"]
            )
            mapping["npe03__Organization__c"] = next(account_id, None)
        elif not mapping.get("npe03__Contact__c") and record.get("contact_name"):
            account_id = (
                r["Id"]
                for r in self.reference_contacts
                if r["Name"] == record["contact_name"]
            )
            mapping["npe03__Contact__c"] = next(account_id, None)
        else:
            raise Exception("No Account or Contact provided for the donation")

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        if record.get("external_id"):
            external_id = record["external_id"]
            mapping[external_id["name"]] = external_id["value"]

        return self.validate_output(mapping)


class CampaignSink(SalesforceV3Sink):
    endpoint = "sobjects/Campaign"
    unified_schema = Campaign
    name = "Campaigns"
    available_names = ["campaigns"]

    def preprocess_record(self, record, context):

        record = self.validate_input(record)

        # fields = self.sf_fields_description

        mapping = {
            "Name": record.get("name"),
            "Type": record.get("type"),
            "Status": record.get("status"),
            "StartDate": record.get('start_date'),
            "EndDate": record.get('end_date'),
            "Description": record.get('description'),
            "IsActive":record.get('active')
        }

        if record.get('id'):
            # If Campaign has an Id will use it to update
            mapping.update({"Id":record['id']})
        else:
            # If no Id we'll use email to search for an existing record
            data = self.query_sobject(
                query = f"SELECT Name,Id,CreatedDate from Campaign WHERE Name = '{record.get('name')}' ORDER BY CreatedDate ASC",
                fields = ['Name','Id']
                )

            if data:
                mapping.update({"Id":data[0].get("Id")})

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        return self.validate_output(mapping)

    def process_record(self, record, context):
        """Process the record."""

        # Getting custom fields from record
        # self.process_custom_fields(record)

        if record.get("Id"):
            fields = ["Id"]
        else:
            fields_dict = self.sf_fields_description()
            fields = fields_dict["external_ids"]

        for field in fields:
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
                    return
                except:
                    self.logger.info(f"{field} with id {record[field]} does not exist. \nWill attepmt to create it.")
                    record = update_record

        if not record.get("Name") and not record.get("WhatId"):
            raise FatalAPIError("ERROR: Campaigns in Salesforce are required to have a 'Name' field")


        try:
            response = self.request_api("POST", request_data=record)
            id = response.json().get("id")
            self.logger.info(f"{self.name} created with id: {id}")
        except Exception as e:
            self.logger.exception("Error encountered while creating campaign")
            raise e


class CampaignMemberSink(SalesforceV3Sink):
    endpoint = "sobjects/CampaignMember"
    unified_schema = None
    name = "CampaignMembers"
    available_names = ["campaignmembers"]

    def preprocess_record(self, record, context) -> dict:

        mapping = {
            "CampaignId": record.get("campaign_id"),
            # "Description": record.get("description"),
            # "HasResponded": record.get("responded",False)
        }

        if record.get('contact_id'):
            if record.get('type') == "contact":
                mapping.update({"ContactId": record.get('contact_id')})
                id = self.get_campaign_member_id(contact_id=record.get('contact_id'),campaign_id=record.get('campaign_id'))
            else:
                mapping.update({"LeadId": record.get('contact_id')})
                id = self.get_campaign_member_id(contact_id=record.get('contact_id'),campaign_id=record.get('campaign_id'),contact_lookup="LeadId")

        if id:
            record['id'] = id

        if record.get('id'):
            # If Campaign has an Id will use it to update
            mapping.update({"Id":record['id']})

        if mapping.get("Id"):
            if "CampaignId" in mapping:
                mapping.pop("CampaignId")
            if "LeadId" in mapping:
                mapping.pop("LeadId")

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        return self.validate_output(mapping)

    def get_campaign_member_id(self, contact_id, campaign_id, contact_lookup = 'ContactId'):

        query = self.query_sobject(
            query=f"SELECT Id, CampaignId, {contact_lookup} from CampaignMember WHERE CampaignId = '{campaign_id}' AND {contact_lookup} = '{contact_id}'",
            fields=['Id']
        )
        if query:
            return query[0]['Id']
        return None


class ActivitiesSink(SalesforceV3Sink):
    endpoint = "sobjects/Task"
    unified_schema = Activity
    name = "Activities"
    available_names = ["activities"]

    def preprocess_record(self, record, context):

        record = self.validate_input(record)

        # fields = self.sf_fields_description

        call_start = record.get('start_datetime')
        call_end = record.get('end_datetime')
        if call_start and call_end:
            call_duration = int(call_end.timestamp() - call_start.timestamp())
        else:
            call_duration = None

        mapping = {
            "Id":record.get('id'),
            "Status": record.get('status'),
            "WhoId": record.get('contact_id'),
            "OwnerId": record.get('owner_id'),
            "WhatId": record.get('related_to'),
            "Subject": record.get('type'),
            "ActivityDate": record.get('activity_datetime'),
            "CallDurationInSeconds": call_duration,
            "Description":record.get("description")
        }

        if record.get("custom_fields"):
            self.process_custom_fields(record["custom_fields"])
            for cf in record.get("custom_fields"):
                if not cf['name'].endswith('__c'):
                    cf['name'] += '__c'
                mapping.update({cf['name']:cf['value']})

        return self.validate_output(mapping)


class FallbackSink(SalesforceV3Sink):
    endpoint = "sobjects/"

    def get_fields_for_object(self, object_type):
        """Check if Salesforce has an object type and fetches its fields."""
        req = self.request_api("GET")
        for object in req.json().get("sobjects", []):
            if object["name"] == object_type or object["label"] == object_type or object["labelPlural"] == object_type:
                obj_req = self.request_api("GET", endpoint=f"sobjects/{object['name']}/describe").json()
                return {f["name"]: f for f in obj_req.get("fields", [])}

        raise MissingObjectInSalesforceError(f"Object type {object_type} not found in Salesforce.")

    def validate_record(self, record, fields):
        new_record = {}
        for original_field, value in record.items():
            if original_field not in fields:
                self.logger.warning(
                    f"Field {original_field} not found in Salesforce. Will not be synced."
                )
                continue

            if fields[original_field]["nillable"] == False and value is None:
                self.logger.warning(
                    f"Field {original_field} is not nullable. Will not be synced."
                )
                continue

            new_record[original_field] = value

        return new_record

    def preprocess_record(self, record, context):
        # Check if object exists in Salesforce
        object_type = None
        req = self.request_api("GET", "sobjects")
        for object in req.json().get("sobjects", []):
            is_name = object["name"] == self.stream_name
            is_label = object["label"] == self.stream_name
            is_label_plural = object["labelPlural"] == self.stream_name
            if is_name or is_label or is_label_plural:
                self.logger.info(f"Processing record for type {self.stream_name}. Using fallback sink.")
                object_type = object["name"]
                break

        if not object_type:
            self.logger.info(f"Skipping record, because {self.stream_name} was not found on Salesforce.")
            return

        try:
            fields = self.get_fields_for_object(object_type)
        except MissingObjectInSalesforceError:
            self.logger.info("Skipping record, because it was not found on Salesforce.")
            return
        record = self.validate_record(record, fields)
        record["object_type"] = object_type
        return record

    def process_record(self, record, context):
        object_type = record.pop("object_type", None)
        self.logger.info(f"Processing record for type {self.stream_name}. Using fallback sink.")

        if record == {}:
            self.logger.info(f"Processing record for type {self.stream_name} failed. Check logs.")
            return

        required_fields = []
        if record.get("Id"):
            fields = ["Id"]
        else:
            fields_desc = self.sf_fields_description(object_type=object_type)
            required_fields = fields_desc["required"]
            list_fields = [field_list for field_list in fields_desc.values()]
            fields = []
            for list_field in list_fields:
                for item in list_field:
                    fields.append(item)

        endpoint = f"sobjects/{object_type}"

        # Checks for required fields
        for field in required_fields:
            if record.get(field) is None:
                self.logger.info(f"Skipping record, because {field} is required.")
                return

        for field in record.keys():
            if field not in fields:
                self.logger.info(f"Field {field} doesn't exist on Salesforce.")


        missing_fields = list(set(fields) - set(record.keys()))

        if len(missing_fields) > 0.5 * len(fields):
            self.logger.info(f"This record may require more fields to be mapped. Missing fields: {missing_fields}")

        if record.get("Id") or record.get("id"):
            object_id = record.pop("Id") or record.pop("id")
            url = "/".join([endpoint, object_type, object_id])
            try:
                response = self.request_api("PATCH", endpoint=url, request_data=record)
                id = response.json().get("id")
                self.logger.info(f"{object_type} updated with id: {id}")
            except Exception as e:
                self.logger.exception(f"Error encountered while updating {object_type}")
                raise e

        else:
            try:
                response = self.request_api("POST", endpoint=endpoint, request_data=record)
                id = response.json().get("id")
                self.logger.info(f"{object_type} created with id: {id}")
            except Exception as e:
                self.logger.exception(f"Error encountered while creating {object_type}")
                raise e

