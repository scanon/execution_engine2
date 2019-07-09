from mongoengine import connect
from lib.execution_engine2.models.models import Job, JobOutput, JobInput

connect("jobs")

job = Job()
output = JobOutput()
input = JobInput()

job.user = "test"
job.authstrat = "workspace"
job.wsid = "1234"

output.version = "1"
output.id = "1234"
output.result = {}

input.wsid = job.wsid
input.name = "SpecialJob"
input.service_ver = "Version123"
input.method = "Method"
input.app_id = "App_id"

job.job_input = input
job.job_output = output


job.save()
