import os
import sys
from typing import Dict

import htcondor
import enum


class HTCondorWrapper:
    class JobStatusCodes(enum.Enum):
        UNEXPANDED = 0
        IDLE = 1
        RUNNING = 2
        REMOVED = 3
        COMPLETED = 4
        HELD = 5
        SUBMISSION_ERROR = 6
        NOT_FOUND = -1

    jsc = {
        "0": "Unexepanded",
        1: "Idle",
        2: "Running",
        3: "Removed",
        4: "Completed",
        5: "Held",
        6: "Submission_err",
        -1: "Not found in condor",
    }

    @staticmethod
    def get_job_status(job):
        pass



    @staticmethod
    def _check_if_not_root():
        if os.geteuid() == 0:
            sys.exit("Cannot run script as root. Need access to htcondor password file")

    @staticmethod
    def get_condor_q_jobs(requirements=None, projection=None) -> Dict[str, Dict]:
        """
        Query the Schedd for all jobs currently stored in condor_q created by NJS
        The jobs are
        * Idle
        * Running
        * Held
        * Possibly Complete
        :param requirements: Condor Specific requirements (e.g. LastJobStatus == 1)
        :return: A dict of condor jobs keyed by UJS-JOB-ID
        """

        HTCondorWrapper._check_if_not_root()


