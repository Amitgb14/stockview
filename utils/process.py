#!/usr/bin/python2.7
# -*- mode:python -*-
import os
import sys
import csv
import redis
import json
import logging
import requests
from config import *
from zipfile import ZipFile
from datetime import datetime

headers = {'user-agent': 'pyx/0.0.1'}  
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
root_path = os.path.dirname(__file__)
LAST = "last_entry"
class Process():
    def __init__(self, filename="config.yml"):
        self.config = config(filename)
        self.connection = self.__connect()

    def __connect(self):
        try:
            r = redis.from_url(os.environ.get("REDIS_URL"))
            logging.info("Connected Redis successfully.")
            return r
        except Exception as e:
            logging.error("Failed to connect Redis servcie: {}".format(e.args))
            sys.exit(1)

    def __reset_connection(self):
        self.connection = self.__connect()

    def download_file(self, filename):
        link = self.config["process"]["link"]
        url = link.format(date=filename)
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            with open("{0}/csv/{1}.zip".format(root_path, filename), 'wb') as f:  
                f.write(r.content)
        else:
            logging.error("Failed to download: {}".format(r.content))
            return False
        return True

    def read_csv(self, csv_filename):
        dict_obj = {}
        with open(csv_filename, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_number = 0
            for row in csv_reader:
                if line_number > 0:
                    dict_obj[row["SC_CODE"]] = {
                        "name": row["SC_NAME"].strip(),
                        "open": float(row["OPEN"].strip()),
                        "high": float(row["HIGH"].strip()),
                        "low": float(row["LOW"].strip()),
                        "close": float(row["CLOSE"].strip()) 
                    }
                else:
                    line_number += 1
        json_data = json.dumps(dict_obj)
        key = os.path.split(csv_filename)[-1]
        return self.__post_data(key, json_data)

    def pull_data(self, filename=None):
        if filename is None:
            filename = datetime.today().strftime('%d%m%y')
        key = self.connection.get(LAST)
        logging.info("{}=>{}".format(key, filename))
        if key and key[2:-4] == filename:
            logging.info("Arleady in DB {0}.".format(filename))
            return True

        target_dir = "{}/csv/".format(root_path)
        zip_file = "{0}/csv/{1}.zip".format(root_path, filename)
        
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        status = self.download_file(filename=filename)
        csv_filename = None

        if status:
            with ZipFile(zip_file, 'r') as zip: 
                zip.extractall(target_dir)
                for info in zip.infolist():
                    csv_filename = target_dir+info.filename
                    s = self.read_csv(target_dir+info.filename)
                    try:
                        os.remove(zip_file)
                        if csv_filename:
                            os.remove(csv_filename)
                    except (OSError, e):
                        logging.error("Failed to cleanup")
                    return s

        return None

    def __post_data(self, key, data):
        s = self.connection.set(key, data)
        if s:
            logging.info("Stored {} key.".format(key))
            self.connection.set(LAST, key)
            return True

    def __sort(self, data, record=10):
        sorted_dict = {}
        sorted_by_high = sorted(data.items(), key=lambda kv: kv[1]['high'], reverse=True)
        l = sorted_by_high[:record]
        return l

    def __search_by_name(self, data, name):
        for (k,v) in data.items():
            if str(v['name']).strip().lower() == name:
                return [(k, v)]
        return

    def get_data(self, key=None, record=10, name=None):
        if key is None:
            key = self.connection.get(LAST)
            if key is None:
                self.pull_data()
        data = self.connection.get(key)
        if data is None:
            logging.error("Record not found '{}'.".format(key))
            return {}
        data = json.loads(data)
        if name is None:
            result = self.__sort(data, record)
        else:
            result = self.__search_by_name(data, name)
        return result
