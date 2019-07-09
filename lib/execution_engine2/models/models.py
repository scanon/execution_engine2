import datetime
from enum import Enum

from mongoengine import StringField, IntField, EmbeddedDocument, Document, DateTimeField, \
    BooleanField, ListField, LongField, EmbeddedDocumentField, GenericEmbeddedDocumentField, \
    DynamicField


class Status(Enum):
    created = 1
    queued = 2
    running = 3
    finshed = 4


status = ['created', 'queued', 'running', 'finished']
authstrat = ['kbaseworkspace', 'something_else']

# TODO Make sure Datetime is correct format
# TODO Use ReferenceField to create a mapping between WSID and JOB IDS?
"""
"created": ISODate("2019-02-19T23:00:57.119Z"),
"updated": ISODate("2019-02-19T23:01:32.132Z"),
"""


class Meta(EmbeddedDocument):
    run_id = StringField()
    token_id = StringField()
    tag = StringField()
    cell_id = StringField()
    status = StringField()


class JobInput(EmbeddedDocument):
    wsid = IntField(required=True)
    method = StringField(required=True)

    requested_release = StringField()
    params = DynamicField()
    service_ver = StringField(required=True)
    app_id = StringField(required=True)

    narrative_cell_info = EmbeddedDocumentField(Meta)


class JobOutput(EmbeddedDocument):
    version = StringField(required=True)
    id = LongField(required=True)
    result = DynamicField(required=True)


class Job(Document):
    user = StringField(required=True)
    authstrat = StringField(required=True, default="kbaseworkspace")
    wsid = IntField(required=True)

    created = DateTimeField(default=datetime.datetime.utcnow)
    updated = DateTimeField(default=datetime.datetime.utcnow)
    started = DateTimeField()

    creation_time = LongField()
    exec_start_time = LongField()
    finish_time = LongField()

    complete = BooleanField(default=False)
    error = BooleanField(default=False)

    errormsg = StringField()

    scheduler_type = StringField()
    scheduler_id = StringField()

    job_input = EmbeddedDocumentField(JobInput)

    job_output = EmbeddedDocumentField(JobOutput)


# Unused fields

result_example = {
    "shocknodes": [],
    "shockurl": "https://ci.kbase.us/services/shock-api/",
    "workspaceids": [],
    "workspaceurl": "https://ci.kbase.us/services/ws/",
    "results": [
        {
            "servtype": "Workspace",
            "url": "https://ci.kbase.us/services/ws/",
            "id": "psnovichkov:1450397093052/QQ",
            "desc": "description"
        }
    ],
    "prog": 0,
    "maxprog": None,
}


class Results(EmbeddedDocument):
    run_id = StringField()
    shockurl = StringField()
    workspaceids = ListField()
    workspaceurl = StringField()
    shocknodes = ListField()


class ResultsResults(EmbeddedDocument):
    run_id = StringField()
    shockurl = StringField()
    workspaceids = StringField()
    workspaceurl = StringField()


{
    "estcompl": None,
    "service": "bsadkhin",
    "desc": "Execution engine job for simpleapp.simple_add",
    "progtype": None,
}
