#!/usr/bin/env python

from lib.execution_engine2.administration import ExecutionEngineJobs

eej = ExecutionEngineJobs()
eej.purge_incomplete_jobs(dry_run=True)
