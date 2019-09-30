#!/usr/bin/env python
import datetime
import os
from configparser import ConfigParser

from pymongo import MongoClient

try:
    from lib.execution_engine2.db.models.models import (
        LogLines, JobLog
    )

except Exception:
    from models import LogLines, JobLog

UNKNOWN = "UNKNOWN"


class MigrateDatabases:
    """
    GET UJS Record, Get corresponding NJS record, combine the two and save them in a new
    collection, using the UJS ID as the primary key
    """

    documents = []
    threshold = 1000
    none_jobs = 0


    def _get_njs_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.njs_host = parser.get("NarrativeJobService", "mongodb-host")
        self.njs_db = parser.get("NarrativeJobService", "mongodb-database")
        self.njs_user = parser.get("NarrativeJobService", "mongodb-user")
        self.njs_pwd = parser.get("NarrativeJobService", "mongodb-pwd")
        self.njs_logs_collection_name = "exec_logs"

        return MongoClient(
            self.njs_host,
            27017,
            username=self.njs_user,
            password=self.njs_pwd,
            authSource=self.njs_db,
            retryWrites=False,
        )

    def __init__(self):
        self.njs = self._get_njs_connection()
        self.logs = []
        self.threshold = 1000

        self.njs_logs = (
            self._get_njs_connection()
                .get_database(self.njs_db)
                .get_collection(self.njs_logs_collection_name)
        )

        self.ee2_logs = self._get_njs_connection().get_database(self.njs_db).get_collection("logs")


    def save_log(self, log):
        self.logs.append(log.to_mongo())
        if len(self.logs) > self.threshold:
            print("INSERTING 1000 ELEMENTS")
            self.ee2_logs.insert_many(self.logs)
            self.logs = []

    def save_remnants(self):
        self.ee2_logs.insert_many(self.logs)
        self.logs = []

    def begin_log_transfer(self):  # flake8: noqa

        logs = self.njs_logs

        logs_cursor = logs.find()

        for log in logs_cursor:
            print(log)




        #self.save_log(log)
        # Save leftover jobs
        #self.save_remnants()

        # TODO SAVE up to 5000 in memory and do a bulk insert
        # a = []
        # a.append(places(**{"name": 'test', "loc": [-87, 101]}))
        # a.append(places(**{"name": 'test', "loc": [-88, 101]}))
        # x = places.objects.insert(a)


c = MigrateDatabases()
c.begin_log_transfer()

