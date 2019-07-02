#!/usr/bin/env python

from lib.execution_engine2.administration import ExecutionEngineJobs

eej = ExecutionEngineJobs()
eej.log_incomplete_jobs()
# eej.purge_incomplete_jobs()
