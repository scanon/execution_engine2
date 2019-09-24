from mongoengine import connect
from lib.execution_engine2.db.models.models import Job, JobOutput, JobInput

connect("jobs")

job = Job()
output = JobOutput()
inputs = JobInput()

job.user = "test"
job.authstrat = "workspace"
job.wsid = "1234"

output.version = "1"
output.id = "1234"
output.result = {}

inputs.wsid = job.wsid
inputs.name = "SpecialJob"
inputs.service_ver = "Version123"
inputs.method = "Method"
inputs.app_id = "App_id"

job.job_input = inputs
job.job_output = output


job.save()
