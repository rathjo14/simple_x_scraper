import os
import ast
import sys
import json
import yaml
import sqlite3
import requests
import pandas as pd
from typing import List
from datetime import datetime, timedelta

from utils.timestamp_checker import TimestampChecker
from utils.database_functions import SqliteTableObject

class XScrapingObject():
    def __init__(self, user : str, start_date : str, end_date : str):
        self.user = user
        self.max_results = 100
        self.start_date = start_date
        self.end_date = end_date

    def create_url(self) -> str:
        max_results_string = "max_results={}".format(self.max_results)
        query_string = "query=from:{} -is:retweet".format(self.user)
        return_fields_string = "tweet.fields={}".format(",".join(self.fields))
        date_range_string = "start_time={}&end_time={}".format(self.start_date, self.end_date)
        url = "https://api.twitter.com/2/tweets/search/recent?{}&{}&{}&{}".format(max_results_string, query_string, return_fields_string, date_range_string)
        return url

    def auth_and_connect(self, bearer_token : str, url : str) -> str:
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def process_config_yaml(self) -> dict:
        with open(os.getenv("X_SCRAPE_YAML_PATH")) as file:
            return yaml.safe_load(file)

    def write_to_yaml(self, res_json : dict, yaml_output_path : str) -> None:
        with open(yaml_output_path, "a") as file:
            yaml.dump(res_json, file)

    def start(self):
        config = self.process_config_yaml()
        bearer_token : str = config["search_x_api"]["bearer_token"]
        self.fields : List[str] = config["search_x_api"]["fields"]
        yaml_output_path : str = config["yaml"]["path"]
        sqlite_table_object : SqliteTableObject = SqliteTableObject(database_filepath = config["sqlite"]["path"],
                                                                    table = config["sqlite"]["table"],
                                                                    columns = config["sqlite"]["columns"],
                                                                    types = config["sqlite"]["types"],
                                                                    primary_keys = config["sqlite"]["primary_keys"])
        url = self.create_url()
        res_json = self.auth_and_connect(bearer_token, url)
        if res_json.get("data"):
            print("# of records: <<<%s>>>" % len(res_json["data"]))
            self.write_to_yaml(res_json, yaml_output_path)
            sqlite_table_object.insert_many([(self.user, data_dict["created_at"], data_dict["text"]) for data_dict in res_json['data']])
        else:
            print("No records gathered!")

if __name__ == "__main__":
    input_timestamp_format = "%Y-%m-%d %H:%M:%S" #args 2 and 3 should be of this timestamp format
    x_timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
    x_seconds_delta = 10 #x end timestamp request delta if no end timestamp provided
    days_cutoff = 7 # we check default 7 day parser history limit from twitter developer basic pla
    user = sys.argv[1]
    input_start_timestamp = sys.argv[2]
    if len(sys.argv) > 3:
       input_end_timestamp = sys.argv[3]
    else:
        input_end_timestamp = (datetime.now() - timedelta(seconds = x_seconds_delta)).strftime(input_timestamp_format)
    tc = TimestampChecker()
    formatted_start_timestamp = tc.datetime_to_str(tc.str_to_datetime(input_start_timestamp, input_timestamp_format), x_timestamp_format)
    formatted_end_timestamp = tc.datetime_to_str(tc.str_to_datetime(input_end_timestamp, input_timestamp_format), x_timestamp_format)
    if not tc.check_cutoff(tc.str_to_datetime(input_start_timestamp, input_timestamp_format), days_cutoff):
        print("ERROR: Input start timestamp (arg 2) is > 7 days. Must be within 7 days of now.")
        sys.exit()
    
    if os.getenv("X_SCRAPE_YAML_PATH") and os.path.exists(os.getenv("X_SCRAPE_YAML_PATH")):
        xso = XScrapingObject(user, formatted_start_timestamp, formatted_end_timestamp)
        xso.start()
    else:
        print("ERROR: Must have X_SCRAPE_YAML_PATH variable set and file created!")
        sys.exit()