import csv
import logging

from airflow.hooks.base_hook import BaseHook
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser


class FacebookAdsHook(BaseHook):
    def __init__(self, facebook_conn_id, api_version):
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version

    def build_values_for_csv(self, values, new_dict: dict, contains_dict=False):

        for value in values:
            if type(value) is list:
                contains_dict = True
                self.build_values_for_csv(
                    values=value, new_dict=new_dict, contains_dict=contains_dict
                )
            elif type(value) is dict:
                value = value["value"]
                new_dict.append(value)
            else:
                new_dict.append(value)

        if not contains_dict:
            new_dict.append(0)

    def build_values_for_csv_set(self, values, new_dict: dict, contains_dict=False):

        for value in values:
            if type(value) is list or type(value) is dict:
                contains_dict = True
                new_dict.append(f'"{str(value)}"')
                continue
            else:
                new_dict.append(value)

        if not contains_dict:
            new_dict.append("[]")

    def build_json_header_for_csv(self, keys, new_dict: dict):
        for key in keys:
            if "outbound_clicks" in key:
                new_dict.append("outbound_clicks")
            else:
                new_dict.append(key)

    def build_file(self, tmp_filepath, parameters, fields):
        conn = self.get_connection(self.facebook_conn_id)
        logging.info(conn.extra_dejson)
        FacebookAdsApi.init(
            conn.extra_dejson["app_id"],
            conn.extra_dejson["app_secret"],
            conn.extra_dejson["access_token"],
            api_version=self.api_version,
        )
        ad_acc = AdAccountUser(fbid="me")
        accounts_id = [account._json["id"]
                       for account in ad_acc.get_ad_accounts()]
        fields_new = []
        for field in fields[0]:
            if field and field != ",":
                fields_new.append(field)
        list_json_response = []
        for account_id in accounts_id:
            type_relat = parameters["level"]
            if type_relat == "ads":
                response = AdAccount(account_id).get_ads(
                    params=parameters, fields=fields_new
                )
            else:
                response = AdAccount(account_id).get_insights(
                    params=parameters, fields=fields_new
                )
            for response_obj in response:
                list_json_response.append(response_obj.export_all_data())

        if list_json_response:
            with open(tmp_filepath, "w", newline="") as f:
                writer = csv.writer(f)
                count = 0
                for item in list_json_response:
                    new_values = []
                    if count == 0:
                        writer.writerow(fields_new)
                    count += 1
                    columns_dict = parameters["columns_dict"]
                    if any(item in fields_new for item in columns_dict):
                        self.build_values_for_csv_set(
                            values=item.values(), new_dict=new_values
                        )
                    else:
                        self.build_values_for_csv(
                            values=item.values(), new_dict=new_values
                        )
                    writer.writerow(new_values)
