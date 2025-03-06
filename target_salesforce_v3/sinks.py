"""SalesforceV3 target sink class, which handles writing streams."""
from __future__ import annotations

import json
import urllib
from target_salesforce_v3.client import SalesforceV3Sink

from hotglue_models_crm.crm import Contact, Company, Deal, Campaign,Activity
from backports.cached_property import cached_property

from dateutil.parser import parse
from datetime import datetime
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from target_salesforce_v3.exceptions import InvalidDealRecord, MissingObjectInSalesforceError




class ContactsSink(SalesforceV3Sink):
    endpoint = "sobjects/Contact"
    unified_schema = Contact
    name = Contact.Stream.name
    campaigns = None
    topics = None
    contact_type = "Contact"
    available_names = ["contacts", "customers"]
    new_custom_fields = []
    
    @cached_property
    def _fields(self):
        return self.get_fields_for_object(self.contact_type)

    @cached_property
    def reference_data(self):
        params = {"q": "SELECT id, name from Account"}
        response = self.request_api("GET", endpoint="query", params=params)
        response = response.json()["records"]
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]
    
    @cached_property
    def campaign_member_fields(self):
        return self.get_fields_for_object("CampaignMember")

    def preprocess_record(self, record: dict, context: dict):
        # 1. Map and process record
        # Parse data
        if isinstance(record.get("addresses"), str):
            record["addresses"] = json.loads(record["addresses"])

        if isinstance(record.get("phone_numbers"), str):
            record["phone_numbers"] = json.loads(record.get("phone_numbers"))

        if isinstance(record.get("campaigns"), str):
            record["campaigns"] = json.loads(record.get("campaigns"))

        if record.get("company") and not record.get("company_name"):
            record["company_name"] = record["company"]

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

        # map data
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
            "HasOptedOutOfEmail": record.get("unsubscribed")
            if record.get("unsubscribed") is not None
            else record.get("subscribe_status") == "unsubscribed",            
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

        # We map tags => topics in Salesforce
        if record.get('tags'):
            self.topics = record['tags']

        # We map campaigns => campaigns in Salesforce
        if record.get('campaigns'):
            self.campaigns = record['campaigns']

        # We map lists => campaigns in Salesforce
        if not self.campaigns and record.get("lists"):
            self.campaigns = [{"name": list_item} for list_item in record.get("lists")]

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
            mapping[f"{_prefix}Country"] = self.map_country(address.get("country"))

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
            mapping["OtherCountry"] = self.map_country(address.get("country"))

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
            '''
            if create_custom_fields flag is on, we create custom fields
             excluding non-custom fields that already exist on Contact or CampaignMember

             This is to protect against the case where non-custom fields are passed in custom_fields
            '''
            campaign_members_fields = self.campaign_member_fields
            existing_fields = list(self._fields.keys())
            existing_fields.extend(list(campaign_members_fields.keys()))
            self.process_custom_fields(record["custom_fields"], exclude_fields=existing_fields)
            fields = self._fields

            # add here the fields that will be sent in campaignmember payload
            mapping["campaign_member_fields"] = {}
            # process custom fields
            custom_fields = {cust["name"]: self.process_custom_field_value(cust["value"]) for cust in record["custom_fields"]}
            for key, value in custom_fields.items():
                # check first if field belongs to campaignmembers
                if campaign_members_fields.get(key):
                    # Check to make sure field is not read-only
                    if campaign_members_fields[key]["createable"] or campaign_members_fields[key]["updateable"]:
                        mapping["campaign_member_fields"][key] = value
                    else:
                        self.logger.warning(f"Field {key} is read-only and cannot be updated.")
                if (fields.get(key) and (fields[key]["createable"] or fields[key]["updateable"] or key.lower() in ["id", "externalid"])) or key.endswith("__r") or fields.get(key+"Id"):
                    mapping[key] = value
                if f"{key}__c" in self.new_custom_fields:
                    mapping.update({f"{key}__c": value})

        if not mapping.get("AccountId") and record.get("company_name"):
            account_id = (
                r["Id"]
                for r in self.reference_data
                if r["Name"] == record["company_name"]
            )
            mapping["AccountId"] = next(account_id, None)

        # validate mapping
        mapping = self.validate_output(mapping)

        # 2. Check if record exist based on default lookup_fields or lookup_fields set in config
        lookup_field = None
        lookup_method = self.config.get("lookup_method", "sequential")
        # check if record already exists
        if record.get('id'):
            # If contact has an Id will use it to updatev
            mapping.update({"Id": record['id']})
            lookup_field = f"Id = '{record['id']}'"
        # if lookup_fields set for unified sinks check those first
        elif self.lookup_fields_dict.get(self.contact_type):
            lookup_values = {
                field: mapping.get(field)
                for field in self.lookup_fields_dict.get(self.contact_type)
                if field in mapping
            }
            if lookup_values:
                lookup_field = self.get_lookup_filter(lookup_values, lookup_method)
        elif record.get("external_id"):
            external_id = record["external_id"]
            mapping[external_id["name"]] = external_id["value"]
            lookup_field = f"{external_id['name']} = '{external_id['value']}'"
        else:
            # If no Id we'll use email to search for an existing record
            if record.get('email'):
                # Get contact_id based on email
                data = self.query_sobject(
                    query = f"SELECT Name, Id from {self.contact_type} WHERE Email = '{record.get('email')}'",
                    fields = ['Name', 'Id']
                )
                if data:
                    id = data[0].get("Id")
                    mapping.update({"Id":id})
                    lookup_field = f"Id = '{id}'"
        
        # If flag only_upsert_empty_fields is true, only upsert empty fields
        if self.config.get("only_upsert_empty_fields") and lookup_field:
            relevant_mapping = {k: v for k, v in mapping.items() if k != "campaign_member_fields"}
            mapping = self.map_only_empty_fields(mapping, self.contact_type, lookup_field)

        return mapping

    def upsert_record(self, record, context):
        """Process the record."""
        state_updates = dict()
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
                    campaign_members_fields = update_record.pop("campaign_member_fields", {})
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
                        self.assign_to_campaign(id, self.campaigns, campaign_members_fields)

                    if self.topics:
                        self.assign_to_topic(id,self.topics)

                    return id, True, state_updates
                except Exception as e:
                    self.logger.exception(f"Could not PATCH to {url}: {e}")
                    raise e
        if record:
            try:
                campaign_members_fields = record.pop("campaign_member_fields", {})
                response = self.request_api("POST", request_data=record)
                id = response.json().get("id")
                self.logger.info(f"{self.contact_type} created with id: {id}")
                # Check for campaigns to be added
                if self.campaigns:
                    self.assign_to_campaign(id,self.campaigns, campaign_members_fields)

                if self.topics:
                    self.assign_to_topic(id,self.topics)
                return id, True, state_updates
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
            elif '[{"message":"No such column \'HasOptedOutOfEmail\' on sobject of type' in response.text:
                self.update_field_permissions(profile = 'System Administrator', sobject_type = self.contact_type, field_name=f"{self.contact_type}.HasOptedOutOfEmail")
                raise RetriableAPIError(f"DEBUG: HasOptedOutOfEmail column was not found, updating 'Field-Leve Security'\n'System Administrator'[x]")
            else:
                try:
                    msg = response.text
                except:
                    msg = self.response_error_message(response)
                raise FatalAPIError(msg)

    def assign_to_topic(self,contact_id,topics:list) -> None:
        """
        This function recieves a contact_id and a list of topics and assigns the contact_id to each topic

        Input:
        contact_id : str
        topics : list[str]
        """

        for topic in topics:
            # data = self.get_query(endpoint=f"sobjects/Campaign/Name/{campaign.get('name')}")
            data = self.query_sobject(
                query = f"SELECT Id, CreatedDate from Topic WHERE Name = '{topic}' ORDER BY CreatedDate ASC",
                fields = ['Id']
                )
            # Extract topic id from record
            if not data:
                self.logger.info(f"No topic found with Name = '{topic}'\nCreating topic ...")
                # Create the topic since it doesn't exist
                response = self.request_api("POST", endpoint="sobjects/Topic", request_data={
                    "Name": topic,
                })
                id = response.json().get("id")
                self.logger.info(f"{self.name} created with id: {id}")
                topic_id = id
            else:
                topic_id = data[0]['Id']

            # Add the contact to the topic
            self.logger.info(f"INFO: Adding Contact/Lead Id:[{contact_id}] to Topic Id:[{topic_id}].")

            try:
                response = self.request_api("POST",endpoint="sobjects/TopicAssignment",request_data={
                    "EntityId": contact_id,
                    "TopicId": topic_id
                })

                data = response.json()
                self.logger.info(f"Added TopicAssignment")
            except Exception as e:
                # Means it's already in the topic
                if "DUPLICATE_VALUE" in str(e):
                    return

                self.logger.exception("Error encountered while creating TopicAssignment")
                raise e

    def assign_to_campaign(self,contact_id,campaigns:list, payload={}) -> None:
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
                    self.logger.info(f"No Campaign found with Name = '{campaign.get('name')}'\nCreating campaign ...")
                    # Create the campaign since it doesn't exist
                    response = self.request_api("POST", endpoint="sobjects/Campaign", request_data={
                        "Name": campaign.get("name"),
                    })
                    id = response.json().get("id")
                    self.logger.info(f"{self.name} created with id: {id}")
                    campaign['campaign_id'] = id
                else:
                    campaign['campaign_id'] = data[0]['Id']

            # Assigns the customer_id to the campaign_id or lead_id
            mapping = {"CampaignId": campaign.get("campaign_id") or campaign.get("id")}
            if self.contact_type == "Contact":
                mapping.update({"ContactId": contact_id})
            else:
                mapping.update({"LeadId": contact_id})
            
            # add custom fields to payload
            if payload:
                mapping.update(payload)

            # Create the CampaignMember
            self.logger.info(f"INFO: Adding Contact/Lead Id:[{contact_id}] as a CampaignMember of Campaign Id:[{mapping.get('CampaignId')}].")

            try:
                response = self.request_api("POST",endpoint="sobjects/CampaignMember",request_data=mapping)
                data = response.json()

                if isinstance(data, list) and data[0].get("message") == "Already a campaign member.":
                    return

                id = data.get("id")
                self.logger.info(f"CampaignMember created with id: {id}")
            except Exception as e:
                self.logger.exception("Error encountered while creating CampaignMember")
                self.logger.exception(f"error: {e}, response: {response.json()}, request body: {response.request.body}")
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
        response = response.json().get("records",[])
        return [{k: v for k, v in r.items() if k in ["Id", "Name"]} for r in response]

    def preprocess_record(self, record, context):
        try:
            has_name = record.get("title")
            has_stage_name = (record.get("pipeline_stage_id") or record.get("status"))
            has_close_date = record.get("close_date")
            if not (has_name and has_stage_name and has_close_date):
                raise InvalidDealRecord(
                                        f'The record does not contain all the necessary fields for creating a new Opportunity: '
                                        f'Name (title: {has_name}), '
                                        f'StageName (pipeline_stage_id: {record.get("pipeline_stage_id")} or status: {record.get("status")}), '
                                        f'CloseDate (close_date: {has_close_date})'
                                    )
        
            if isinstance(record.get("custom_fields"), str):
                try:
                    record["custom_fields"] = json.loads(record["custom_fields"])
                except:
                    self.logger.info(f"custom_fields is not a valid Json document: {record['custom_fields']}")

            record = self.validate_input(record)

            record_stage = record.get("pipeline_stage_id")
            if not record_stage:
                record_stage = record.get("status") # fallback on field

            record_stage = self.get_pickable(record_stage, "StageName", select_first=True)

            record_type = record.get("type")
            record_type = self.get_pickable(record_type, "Type")

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
                "StageName": record_stage,
                "CloseDate": record["close_date"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "Description": record.get("description"),
                "Type": record_type,
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
                    mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

            # 2. get record using lookup_fields
            lookup_field = None
            lookup_method = self.config.get("lookup_method", "sequential")
            if self.lookup_fields_dict.get(self.name):
                lookup_values = {
                    field: mapping.get(field)
                    for field in self.lookup_fields_dict.get(self.name)
                    if field in mapping
                }
                if lookup_values:
                    lookup_field = self.get_lookup_filter(lookup_values, lookup_method)
            elif record.get("external_id"):
                external_id = record["external_id"]
                mapping[external_id["name"]] = external_id["value"]
                lookup_field = f'{external_id["name"]} = {external_id["value"]}'

            mapping = self.validate_output(mapping)

            # If flag only_upsert_empty_fields is true, only upsert empty fields
            if self.config.get("only_upsert_empty_fields") and lookup_field:
                mapping = self.map_only_empty_fields(mapping, "Opportunity", lookup_field)

            return mapping
        except Exception as exc:
            return {"error": repr(exc)}

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
            mapping["BillingCountry"] = self.map_country(address.get("country"))

        if record.get("addresses") and len(record["addresses"]) >= 2:
            address = record["addresses"][1]
            street = "\n".join([v for k, v in address if "line" in k and v is not None])
            mapping["ShippingStreet"] = street
            mapping["ShippingCity"] = address.get("city")
            mapping["ShippingState"] = address.get("state")
            mapping["ShippingPostalCode"] = address.get("postal_code")
            mapping["ShippingCountry"] = self.map_country(address.get("country"))

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
                mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

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
        installment_period = None
        if record.get("installment_period"):
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
                mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

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
        # 1. Process record
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
                mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

        mapping = self.validate_output(mapping)

        # 2. Get record using lookup_fields
        lookup_field = None
        lookup_method = self.config.get("lookup_method", "sequential")
        if self.lookup_fields_dict.get(self.name):
            lookup_values = {
                field: mapping.get(field)
                for field in self.lookup_fields_dict.get(self.name)
                if field in mapping
            }
            if lookup_values:
                lookup_field = self.get_lookup_filter(lookup_values, lookup_method)
        elif mapping.get("Id"):
            lookup_field = f"Id = {mapping['Id']}"

        # If flag only_upsert_empty_fields is true, only upsert empty fields
        if self.config.get("only_upsert_empty_fields") and lookup_field:
            mapping = self.map_only_empty_fields(mapping, "Campaign", lookup_field)
        return mapping


    def upsert_record(self, record, context):
        """Process the record."""

        state_updates = dict()
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
                    return id, True, state_updates
                except:
                    self.logger.info(f"{field} with id {record[field]} does not exist. \nWill attepmt to create it.")
                    record = update_record

        if not record.get("Name") and not record.get("WhatId"):
            raise FatalAPIError("ERROR: Campaigns in Salesforce are required to have a 'Name' field")


        try:
            response = self.request_api("POST", request_data=record)
            id = response.json().get("id")
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates
        except Exception as e:
            self.logger.exception("Error encountered while creating campaign")
            raise e


class CampaignMemberSink(SalesforceV3Sink):
    endpoint = "sobjects/CampaignMember"
    unified_schema = None
    name = "CampaignMembers"
    available_names = ["campaignmembers"]

    def preprocess_record(self, record, context) -> dict:
        # 1. Map and process record
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
                mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

        mapping = self.validate_output(mapping)
        
        # 2. Get record using lookup_fields
        lookup_field = None
        lookup_method = self.config.get("lookup_method", "sequential")
        if self.lookup_fields_dict.get(self.name):
            lookup_values = {
                field: mapping.get(field)
                for field in self.lookup_fields_dict.get(self.name)
                if field in mapping
            }
            if lookup_values:
                lookup_field = self.get_lookup_filter(lookup_values, lookup_method)
        elif mapping.get("Id"):
            lookup_field = f"Id = {mapping['Id']}"

        # If flag only_upsert_empty_fields is true, only upsert empty fields
        if self.config.get("only_upsert_empty_fields") and lookup_field:
            mapping = self.map_only_empty_fields(mapping, "CampaignMember", lookup_field)

        return mapping

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
        # 1. Map and process record 
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
                mapping.update({cf['name']:self.process_custom_field_value(cf['value'])})

        mapping = self.validate_output(mapping)

        # 2. Get record using lookup_fields
        lookup_field = None
        lookup_method = self.config.get("lookup_method", "sequential")
        if self.lookup_fields_dict.get(self.name):
            lookup_values = {
                field: mapping.get(field)
                for field in self.lookup_fields_dict.get(self.name)
                if field in mapping
            }
            if lookup_values:
                lookup_field = self.get_lookup_filter(lookup_values, lookup_method)
        elif mapping.get("Id"):
            lookup_field = f"Id = {mapping['Id']}"

        # If flag only_upsert_empty_fields is true, only upsert empty fields
        if self.config.get("only_upsert_empty_fields") and lookup_field:
            mapping = self.map_only_empty_fields(mapping, "Task", lookup_field)

        return mapping


class FallbackSink(SalesforceV3Sink):
    endpoint = "sobjects/"

    @property
    def name(self):
        return self.stream_name
    
    def get_record(self, lookup_values, object_type, fields, record, method):
        # get select fields for query
        query_fields = [field for field in fields.keys() if field in record]
        # add Id field to query_fields if it's not there
        if "Id" not in query_fields:
            query_fields.append("Id")
        query_fields = ",".join(query_fields)

        # get filter for query
        query_filter = self.get_lookup_filter(lookup_values, method)
        # execute query
        query = f"SELECT {query_fields} FROM {object_type} WHERE {query_filter}"
        req = self.request_api("GET", "queryAll", params={"q": query})
        req = req.json().get("records")
        return req
    not_searchable_by_mail = ["ContentVersion"]

    def get_fields_for_object(self, object_type):
        """Check if Salesforce has an object type and fetches its fields."""
        req = self.request_api("GET")
        for object in req.json().get("sobjects", []):
            if object["name"] == object_type or object["label"] == object_type or object["labelPlural"] == object_type:
                obj_req = self.request_api("GET", endpoint=f"sobjects/{object['name']}/describe").json()
                return {f["name"]: f for f in obj_req.get("fields", [])}

        raise MissingObjectInSalesforceError(f"Object type {object_type} not found in Salesforce.")

    def preprocess_record(self, record, context):
        # Check if object exists in Salesforce
        object_type = None
        req = self.request_api("GET", "sobjects")
        objects_list = req.json().get("sobjects", [])
        for object in objects_list:
            is_name = object["name"] == self.stream_name
            is_label = object["label"] == self.stream_name
            is_label_plural = object["labelPlural"] == self.stream_name
            if is_name or is_label or is_label_plural:
                self.logger.info(f"Processing record for type {self.stream_name}. Using fallback sink.")
                object_type = object["name"]
                break

        if not object_type:
            self.logger.info(f"Record doesn't exist on Salesforce {self.stream_name} was not found on Salesforce.")
            return {}

        try:
            fields = self.get_fields_for_object(object_type)
        except MissingObjectInSalesforceError:
            self.logger.info("Skipping record, because it was not found on Salesforce.")
            return {}
        
        # add field to link attachments
        if self.name == "ContentVersion":
            fields.update({"LinkedEntityId": {"createable": True}})
        
        # keep only available fields and that are creatable or updatable
        # NOTE: we need to keep relations (__r, xId)
        record = {k:v for k,v in record.items() if k.endswith("__r") or fields.get(k+"Id") or (fields.get(k) and (fields[k]["createable"] or fields[k]["updateable"] or k.lower() in ["id", "externalid"]))}
        # clean empty date fields to avoid salesforce parsing error
        record = {k:v for k,v in record.items() if fields.get(k, {}).get("type") not in ["date", "datetime"] or (fields.get(k, {}).get("type") in ["date", "datetime"] and v)}
        
        # add object_type
        record["object_type"] = object_type

        # If lookup_fields dict exist in config use it to check if the record exists in Salesforce
        _lookup_fields = self.lookup_fields_dict.get(object_type) or []
        lookup_method = self.config.get("lookup_method", "sequential")
        # Standarize lookup fields as a list of strings
        if isinstance(_lookup_fields, str):
            _lookup_fields = [_lookup_fields]
        # check if the lookup fields exist for the object
        _lookup_fields = [field for field in _lookup_fields if field in fields]
        # get all the values for all lookup fields
        lookup_values = {field: record.get(field) for field in _lookup_fields}

        req = None
        # lookup for record with field from config
        if lookup_values:
            if lookup_method == "all" and len(lookup_values) != len(_lookup_fields):
                self.logger.info(f"Skipping lookup for record {record} because lookup_method is set to 'all' but not all fields have valid values")
            else:
                req = self.get_record(lookup_values, object_type, fields, record, lookup_method)

        # lookup for record with email fields
        elif self.config.get("lookup_by_email", True) and self.name not in self.not_searchable_by_mail:
            # Try to find object instance using email
            email_fields = ["Email", "npe01__AlternateEmail__c", "npe01__HomeEmail__c", "npe01__Preferred_Email__c", "npe01__WorkEmail__c"]
            email_values = [record.get(email_field) for email_field in email_fields if record.get(email_field)]

            for email_to_check in email_values:
                if not email_to_check:
                    continue

                # Escape special characters on email
                for char in ["+", "-"]:
                    if char in email_to_check:
                        email_to_check = email_to_check.replace(char, f"\{char}")

                query = "".join(["FIND {", email_to_check, "} ", f" IN ALL FIELDS RETURNING {object_type}(id)"])
                req = None
                try:
                    lookup_res = self.request_api("GET", "search/", params={"q": query})
                    req = lookup_res.json().get("searchRecords")
                except:
                    self.logger.warning(f"Failed to lookup email field.")
                    continue

                if req:
                    break

        # if record already exists add its Id for patching           
        if req:
            existing_record = req[0]
            # if flag only_upsert_empty_fields is true, only send fields with currently empty values
            if self.config.get("only_upsert_empty_fields"):
                record = {k:v for k,v in record.items() if not existing_record.get(k)}
            record["Id"] = existing_record["Id"]

        # convert any datetimes to string to avoid json encoding errors
        for key in record:
            if isinstance(record.get(key), datetime):
                record[key] = record[key].isoformat()

        # clean any read only fields
        for field in self._target.read_only_fields.get(self.stream_name, []):
            record.pop(field, None)
        return record

    def upsert_record(self, record, context):
        if record == {} or record is None:
            return None, False, {}

        state_updates = dict()

        object_type = record.pop("object_type", None)
        self.logger.info(f"Processing record for type {self.stream_name}. Using fallback sink.")

        if record == {}:
            self.logger.info(f"Processing record for type {self.stream_name} failed. Check logs.")
            return
        
        # for files pop object id to link the file
        linked_object_id = record.pop("LinkedEntityId", None) if self.name == "ContentVersion" else None

        # get object fields
        fields_desc = self.sf_fields_description(object_type=object_type)

        possible_update_fields = []

        for field in fields_desc["external_ids"]:
            if field in record:
                possible_update_fields.append(field)

        # grab the externalId we should use for the state
        if record.get("externalId"):
            state_updates["externalId"] = record["externalId"]
        # TODO: in most cases this would be 1, but what if there's more?
        elif len(possible_update_fields) > 0:
            state_updates["externalId"] = record[possible_update_fields[0]]

        if record.get("Id"):
            fields = ["Id"]
        else:
            list_fields = [field_list for field_list in fields_desc.values()]
            fields = []
            for list_field in list_fields:
                for item in list_field:
                    fields.append(item)

        endpoint = f"sobjects/{object_type}"

        for field in record.keys():
            if field not in fields:
                self.logger.info(f"Field {field} doesn't exist on Salesforce.")


        missing_fields = list(set(fields) - set(record.keys()))

        if len(missing_fields) > 0.5 * len(fields):
            self.logger.info(f"This record may require more fields to be mapped. Missing fields: {missing_fields}")

        if record.get("Id") or record.get("id"):
            object_id = record.pop("Id") or record.pop("id")
            url = "/".join([endpoint, object_id])
            try:
                response = self.request_api("PATCH", endpoint=url, request_data=record)
                if response.status_code == 204:
                    self.logger.info(f"{object_type} updated with id: {object_id}")
                    return object_id, True, state_updates

                id = response.json().get("id")
                self.link_attachment_to_object(id, linked_object_id)
                self.logger.info(f"{object_type} updated with id: {id}")
                return id, True, state_updates
            except Exception as e:
                self.logger.exception(f"Error encountered while updating {object_type}")

        if len(possible_update_fields) > 0:
            for id_field in possible_update_fields:
                try:
                    url = "/".join([endpoint, id_field, record.get(id_field)])
                    response = self.request_api("PATCH", endpoint=url, request_data={k: record[k] for k in set(list(record.keys())) - set([id_field])})
                    id = response.json().get("id")
                    self.logger.info(f"{object_type} updated with id: {id}")
                    self.link_attachment_to_object(id, linked_object_id)
                    return id, True, state_updates
                except Exception as e:
                    self.logger.exception(f"Could not PATCH to {url}: {e}")

        try:
            if len(possible_update_fields) > 0:
                self.logger.info("Failed to find updatable entity, trying to create it.")

            response = self.request_api("POST", endpoint=endpoint, request_data=record)
            id = response.json().get("id")
            self.logger.info(f"{object_type} created with id: {id}")
            self.link_attachment_to_object(id, linked_object_id)
            return id, True, state_updates
        except Exception as e:
            if "INVALID_FIELD_FOR_INSERT_UPDATE" in str(e):
                try:
                    fields = json.loads(str(e))[0]['fields']
                except:
                    raise Exception(f"Attempted to write read-only fields. Unable to extract read-only fields to retry request: {str(e)}")
                
                self.logger.warning(f"Attempted to write read-only fields: {fields}. Removing them and retrying.")
                # append read-only field to a list
                if not self._target.read_only_fields.get(self.stream_name):
                    self._target.read_only_fields[self.stream_name] = []
                self._target.read_only_fields[self.stream_name].extend(fields)
                # remove read-only fields from record
                for f in fields:
                    record.pop(f, None)
                # retry
                response = self.request_api("POST", endpoint=endpoint, request_data=record)
                id = response.json().get("id")
                self.logger.info(f"{object_type} created with id: {id}")
                return id, True, state_updates

            self.logger.exception(f"Error encountered while creating {object_type}")
            raise e


    def link_attachment_to_object(self, file_id, linked_object_id):
        if self.name != "ContentVersion":
            return
        if not linked_object_id:
            self.logger.info(f"Object id not found to link file with id {file_id}")
            return
        try:
            # get contentdocumentid
            content_endpoint = "query"
            params = {"q": f"SELECT ContentDocumentId FROM ContentVersion WHERE Id = '{file_id}'"}
            content_document_id = self.request_api("GET", endpoint=content_endpoint, params=params)
            content_document_id = content_document_id.json()
            if content_document_id.get("records"):
                content_document_id = content_document_id["records"][0]["ContentDocumentId"]
            else:
                raise Exception(f"Failed while trying to link file {file_id} and object {linked_object_id} because ContentDocumentId was not found")

            if isinstance(linked_object_id, dict):
                # they're using an external id, we need to look it up
                link = list(linked_object_id.keys())[0]
                sobject, external_id = link.split("/")
                params = {"q": f"SELECT Id FROM {sobject} WHERE {external_id} = '{linked_object_id[link]}'"}
                link_obj = self.request_api("GET", endpoint=content_endpoint, params=params)
                link_obj = link_obj.json()
                if link_obj.get("records"):
                    linked_object_id = link_obj['records'][0]['Id']
                else:
                    raise Exception(f"Could not find matching {sobject} with {external_id} = '{linked_object_id[link]}'")

            endpoint = "sobjects/ContentDocumentLink"
            record = {
                "ContentDocumentId": content_document_id,
                "LinkedEntityId": linked_object_id,
                "ShareType": "V"
            }
            response = self.request_api("POST", endpoint=endpoint, request_data=record)
            self.logger.info(f"File with id {file_id} succesfully linked to object with id {linked_object_id}. Link id {response.json()['id']}")
        except Exception as e:
            self.logger.info(f"Failed while trying to link file {file_id} and object {linked_object_id}")
            raise e