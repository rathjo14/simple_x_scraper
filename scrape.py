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

class XScrapingObject():
    def __init__(self, user : str, start_date : str, end_date : str):
        self.user = user
        self.max_results = 100
        self.start_date = start_date
        self.end_date = end_date

    def create_url(self) -> str:
        max_results_string = "max_results={}".format(self.max_results)
        query_string = "query=from:{} -is:retweet -is:reply".format(self.user)
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

    def write_to_sqlite(self,  data : dict, database_path : str, table : str, columns : List[str]):
        if not data.get("data"):
            print("No data!")
        else:
            sqlite_connection = sqlite3.connect(database_path)
            sqlite_cursor = sqlite_connection.cursor()
            conflict_string = "ON CONFLICT({}) DO UPDATE SET {}".format(",".join(columns), ",".join([column + "=excluded." + column for column in columns]))
            upsert_string = "INSERT INTO {} VALUES({}) {}".format(table, ",".join(["?"]*len(columns)), conflict_string)
            data_array = [(self.user, data_dict["created_at"], data_dict["text"]) for data_dict in data['data']] #formatting data we get for sqlite insertion
            print("UPSERT STRING: <<<%s>>>" % upsert_string)
            print("# OF POINTS GATHERED: <%s>" % len(data_array))
            # print("DATA ARRAY: <<<%s>>>" % data_array)
            sqlite_cursor.executemany(upsert_string, data_array)
            sqlite_connection.commit()
            sqlite_connection.close()

    def start(self):
        config = self.process_config_yaml()
        bearer_token : str = config["search_x_api"]["bearer_token"]
        self.fields : List[str] = config["search_x_api"]["fields"]
        yaml_output_path : str = config["yaml"]["path"]
        sqlite_database : str = config["sqlite"]["path"]
        sqlite_table : str = config["sqlite"]["table"]
        sqlite_columns : List[str] = config["sqlite"]["columns"]
        url = self.create_url()
        res_json = self.auth_and_connect(bearer_token, url)
        self.write_to_yaml(res_json, yaml_output_path)
        self.write_to_sqlite(res_json, sqlite_database, sqlite_table, sqlite_columns)
        print("Done.")

if __name__ == "__main__":
    timestamp_format = "%Y-%m-%d %H:%M:%S" #args 2 and 3 should be of this timestamp format
    x_seconds_delta = 10 #x end timestamp request delta if no end timestamp provided
    user = sys.argv[1]
    input_start_timestamp = sys.argv[2]
    input_end_timestamp = sys.argv[3] if len(sys.argv) > 3 else (datetime.now() - timedelta(seconds = x_seconds_delta)).strftime(timestamp_format)
    tc = TimestampChecker(timestamp_string_format = "%Y-%m-%d %H:%M:%S")
    formatted_start_timestamp = tc.check_format(input_start_timestamp)
    formatted_end_timestamp = tc.check_format(input_end_timestamp)
    if not tc.check_cutoff(input_start_timestamp):
        print("ERROR: Input start timestamp (arg 2) is > 7 days. Must be within 7 days of now.")
        sys.exit()
    
    if os.getenv("X_SCRAPE_YAML_PATH") and os.path.exists(os.getenv("X_SCRAPE_YAML_PATH")):
        xso = XScrapingObject(user, formatted_start_timestamp, formatted_end_timestamp)
        xso.start()
    else:
        print("ERROR: Must have X_SCRAPE_YAML_PATH variable set and file created!")
        sys.exit()