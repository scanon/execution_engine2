#!/usr/bin/env python
import datetime
import os
from configparser import ConfigParser
from pymongo import MongoClient

jobs_database_name = "ee2_jobs"


try:
    from lib.execution_engine2.db.models.models import (
        Job,
        Status,
        ErrorCode,
        JobInput,
        Meta,
        TerminatedCode,
    )

except Exception:
    from models import Job, Status, ErrorCode, JobInput, Meta, TerminatedCode


UNKNOWN = "UNKNOWN"


class MigrateDatabases:
    """
    GET UJS Record, Get corresponding NJS record, combine the two and save them in a new
    collection, using the UJS ID as the primary key
    """

    documents = []
    threshold = 1000
    none_jobs = 0

    def _get_ujs_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.ujs_host = parser.get("NarrativeJobService", "ujs-mongodb-host")
        self.ujs_db = parser.get("NarrativeJobService", "ujs-mongodb-database")
        self.ujs_user = parser.get("NarrativeJobService", "ujs-mongodb-user")
        self.ujs_pwd = parser.get("NarrativeJobService", "ujs-mongodb-pwd")
        self.ujs_jobs_collection = "jobstate"
        return MongoClient(
            self.ujs_host,
            27017,
            username=self.ujs_user,
            password=self.ujs_pwd,
            authSource=self.ujs_db,
        )

    def _get_njs_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.njs_host = parser.get("NarrativeJobService", "mongodb-host")
        self.njs_db = parser.get("NarrativeJobService", "mongodb-database")
        self.njs_user = parser.get("NarrativeJobService", "mongodb-user")
        self.njs_pwd = parser.get("NarrativeJobService", "mongodb-pwd")
        self.njs_jobs_collection_name = "exec_tasks"
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
        self.ujs = self._get_ujs_connection()
        self.jobs = []
        self.threshold = 1000

        self.ujs_jobs = (
            self._get_ujs_connection()
            .get_database(self.ujs_db)
            .get_collection(self.ujs_jobs_collection)
        )
        self.njs_jobs = (
            self._get_njs_connection()
            .get_database(self.njs_db)
            .get_collection(self.njs_jobs_collection_name)
        )

        self.ee2_jobs = self._get_njs_connection().get_database(self.njs_db).get_collection(jobs_database_name)

        config = {'mongo-host' : self.njs_host,
                  'mongo-port' : 27017,
                  'mongo-database' : self.njs_db,
                  'mongo-user' : self.njs_user,
                  'mongo-password' : self.njs_pwd,
                  'mongo-authmechanism' : "DEFAULT"}
        # self.mongo_util = MongoUtil(config=config)

    def get_njs_job_input(self, njs_job):
        job_input = njs_job.get("job_input")
        if job_input is None:
            self.none_jobs += 1
            print(
                "Found ujs job with corresponding njs job with no job input ",
                self.none_jobs,
                njs_job["ujs_job_id"],
            )
            job_input = {
                "service_ver": UNKNOWN,
                "method": UNKNOWN,
                "app_id": UNKNOWN,
                "params": UNKNOWN,
                "wsid": -1,
            }

        if type(job_input) is list:
            return job_input[0]

        return job_input

    def save_job(self, job):
        self.jobs.append(job.to_mongo())
        if len(self.jobs) > self.threshold:
            print("INSERTING 1000 ELEMENTS")
            self.ee2_jobs.insert_many(self.jobs)
            self.jobs = []

    def save_remnants(self):
        self.ee2_jobs.insert_many(self.jobs)
        self.jobs = []

    def begin_job_transfer(self):  # flake8: noqa
        ujs_jobs = self.ujs_jobs
        njs_jobs = self.njs_jobs

        ujs_cursor = ujs_jobs.find()
        count = 0
        njs_count = 0
        for ujs_job in ujs_cursor:
            count += 1

            ujs_id = str(ujs_job["_id"])
            print(f"Working on job {ujs_id} {count}")

            njs_job = njs_jobs.find_one({"ujs_job_id": {"$eq": ujs_id}})

            job = Job()

            job.id = ujs_id
            job.user = ujs_job["user"]
            job.authstrat = ujs_job["authstrat"]
            authstrat = ujs_job.get("strat")
            authparam = ujs_job.get("authparam")
            if authstrat != "DEFAULT" and authparam != "DEFAULT":
                job.wsid = authparam
            else:
                job.wsid = -1

            if njs_job is not None:
                njs_job_input = self.get_njs_job_input(njs_job)
                job.wsid = njs_job_input.get("wsid", -1)

            complete = ujs_job.get("complete")
            error = ujs_job.get("error")

            if error:
                job.status = Status.error.value
                job.errormsg = ujs_job.get("errormsg")
                job.error_code = ErrorCode.unknown_error.value
            elif complete:
                job.status = Status.finished.value
                job.errormsg = None

            job.updated = ujs_job.get("updated")
            job.running = ujs_job.get("started")
            job.estimating = None
            job.finished = None

            status = ujs_job.get("status")

            exec_start_time = 0.0
            finish_time = 0.0

            if njs_job is not None:
                finish_time = njs_job.get("finish_time", 0.0) / 1000.0
                exec_start_time = njs_job.get("exec_start_time", 0.0) / 1000.0

            if status == "canceled by user":
                job.status = Status.terminated.value
                job.terminated_code = TerminatedCode.terminated_by_user.value
                if njs_job is not None:
                    job.finished = finish_time
                    job.running = exec_start_time
            elif status == "done":
                job.status = Status.finished.value
                if njs_job is not None:
                    job.finished = finish_time
                    job.running = exec_start_time
            # Jobs shouldn't be queued from long ago.. And we shouldn't migrate running/queued jobs probably
            elif status == "error" or status == "queued" or status == "in-progress":
                job.status = Status.error.value
                job.error_code = ErrorCode.unknown_error.value
                if njs_job is not None:
                    job.finished = finish_time
                    job.running = exec_start_time

            elif status is None:
                print(f"{job.id} has a broken status")
                job.status = Status.error.value
                job.error_code = ErrorCode.unknown_error.value
                if njs_job is not None:
                    job.finished = finish_time
                    job.running = exec_start_time

            msg = [ujs_job.get("status", ""), ujs_job.get("desc", "")]
            job.msg = " ".join(msg)
            job.results = ujs_job.get("results")

            job.job_input = None
            job.job_output = None
            job.scheduler_type = "awe"

            if njs_job is None:
                # Just to make validation pass for job input part
                if job.job_input is None:
                    job_input = JobInput()
                    job_input.wsid = -1
                    job_input.method = "0.0"
                    job_input.service_ver = "0"
                    job_input.app_id = "0"
                    meta = Meta()
                    job_input.narrative_cell_info = meta
                    job.job_input = job_input
                    job.validate()
                    job.job_input = None
                else:
                    job.validate()
            else:
                njs_count += 1
                job_input = JobInput()
                njs_job_input = self.get_njs_job_input(njs_job)
                job_input.wsid = int(njs_job_input.get("wsid", -1))
                job_input.method = njs_job_input.get("method", UNKNOWN)
                job_input.requested_release = njs_job_input.get(
                    "requested_release", UNKNOWN
                )
                job_input.service_ver = njs_job_input.get("service_ver", UNKNOWN)
                job_input.app_id = njs_job_input.get(
                    "app_id", njs_job_input.get("method")
                )
                job_input.params = njs_job_input.get("params", {})
                # TODO fix this source ws?
                job_input.source_ws_objects = njs_job_input.get("source_ws_objects")
                job_input.parent_job_id = njs_job_input.get("parent_job_id")
                # job_input.requirements = None
                m = Meta()
                njs_meta = njs_job_input.get("meta")
                if njs_meta:
                    m.run_id = njs_meta.get("run_id")
                    m.token_id = njs_meta.get("token_id")
                    m.tag = njs_meta.get("tag")
                    m.cell_id = njs_meta.get("cell_id")
                    job_input.narrative_cell_info = m
                else:
                    m.run_id = None
                    m.token_id = None
                    m.tag = None
                    m.cell_id = None
                    job_input.narrative_cell_info = m

                if njs_job.get("scheduler_type") is not None:
                    job.scheduler_type = njs_job.get("scheduler_type")
                if njs_job.get("task_id") is not None:
                    job.scheduler_id = njs_job.get("task_id")

                job.job_input = job_input
                job.job_output = njs_job.get("job_output")
                job.validate()

            self.save_job(job)
            # Save leftover jobs
        self.save_remnants()

        # TODO SAVE up to 5000 in memory and do a bulk insert
        # a = []
        # a.append(places(**{"name": 'test', "loc": [-87, 101]}))
        # a.append(places(**{"name": 'test', "loc": [-88, 101]}))
        # x = places.objects.insert(a)


c = MigrateDatabases()
c.begin_job_transfer()

# Interesting jobs on CI
# 564a4fd6e4b0d9c152289eac
# 564a64c6e4b0d9c152289eb6
# 564b3fc2e4b0d9c152289eba
# 5c79bee8e4b0f2ea4c0bae9b
# 5d163bb5aa5a4d298c5dc5e4 queued
# 5d1e477caa5a4d298c5dc601 in-prog

# On prod
# 58108d00e4b0c101e12f6a1f
