module execution_engine2 {

    /* @range [0,1] */
    typedef int boolean;
    /*
        A time in the format YYYY-MM-DDThh:mm:ssZ, where Z is either the
        character Z (representing the UTC timezone) or the difference
        in time to UTC in the format +/-HHMM, eg:
            2012-12-17T23:24:06-0500 (EST time)
            2013-04-03T08:56:32+0000 (UTC time)
            2013-04-03T08:56:32Z (UTC time)
    */
    typedef string timestamp;
    /* A job id. */
    typedef string job_id;
    typedef structure {
        boolean reboot_mode;
        boolean stopping_mode;
        int running_tasks_total;
        mapping<string, int> running_tasks_per_user;
        int tasks_in_queue;
        mapping<string, string> config;
        string git_commit;
    } Status;
    funcdef list_config() returns (mapping<string, string>) authentication optional;
    /* Returns the current running version of the NarrativeJobService. */
    funcdef ver() returns (string);
    /* Simply check the status of this service to see queue details */
    funcdef status() returns (Status);
    /*================================================================================*/
    /*  Running long running methods through Docker images of services from Registry  */
    /*================================================================================*/
    /* A workspace object reference of the form X/Y/Z, where
       X is the workspace name or id,
       Y is the object name or id,
       Z is the version, which is optional.
     */
    typedef string wsref;
    /*
        time - the time the call was started;
        method - service defined in standard JSON RPC way, typically it's
            module name from spec-file followed by '.' and name of funcdef
            from spec-file corresponding to running method (e.g.
            'KBaseTrees.construct_species_tree' from trees service);
        job_id - job id if method is asynchronous (optional field).
    */
    typedef structure {
        timestamp time;
        string method;
        job_id job_id;
    } MethodCall;
    /*
        call_stack - upstream calls details including nested service calls and
            parent jobs where calls are listed in order from outer to inner.
    */
    typedef structure {
        list<MethodCall> call_stack;
        string run_id;
    } RpcContext;
    /*
        method - service defined in standard JSON RPC way, typically it's
            module name from spec-file followed by '.' and name of funcdef
            from spec-file corresponding to running method (e.g.
            'KBaseTrees.construct_species_tree' from trees service);
        params - the parameters of the method that performed this call;

        Optional parameters:
        service_ver - specific version of deployed service, last version is
            used if this parameter is not defined
        rpc_context - context of current method call including nested call
            history
        remote_url - run remote service call instead of local command line
            execution.
        source_ws_objects - denotes the workspace objects that will serve as a
            source of data when running the SDK method. These references will
            be added to the autogenerated provenance.
        app_id - the id of the Narrative application running this job (e.g.
            repo/name)
        mapping<string, string> meta - user defined metadata to associate with
            the job. This data is passed to the User and Job State (UJS)
            service.
        wsid - a workspace id to associate with the job. This is passed to the
            UJS service, which will share the job based on the permissions of
            the workspace rather than UJS ACLs.
        parent_job_id - UJS id of the parent of a batch job. Sub jobs will add
        this id to the NJS database under the field "parent_job_id"
    */
    typedef structure {
        string method;
        list<UnspecifiedObject> params;
        string service_ver;
        RpcContext rpc_context;
        string remote_url;
        list<wsref> source_ws_objects;
        string app_id;
        mapping<string, string> meta;
        int wsid;
        string parent_job_id;
    } RunJobParams;
    /*
        Start a new job (long running method of service registered in ServiceRegistery).
        Such job runs Docker image for this service in script mode.
    */
    funcdef run_job(RunJobParams params) returns (job_id job_id) authentication required;


    /*
        Get job params necessary for job execution
    */
    funcdef get_job_params(job_id job_id) returns (RunJobParams params) authentication required;
    /*
        is_started - optional flag marking job as started (and triggering exec_start_time
            statistics to be stored).
    */
    /*
    typedef structure {
        job_id job_id;
        boolean is_started;
    } UpdateJobParams;
    typedef structure {
        list<string> messages;
    } UpdateJobResults;
    funcdef update_job(UpdateJobParams params) returns (UpdateJobResults)
        authentication required;
    */

    typedef structure {
        job_id job_id;
        string status;
    } UpdateJobStatusParams;

    funcdef update_job_status(UpdateJobStatusParams params) returns (job_id job_id)
        authentication required;
    typedef structure {
        string line;
        boolean is_error;
        string ts;
    } LogLine;
    funcdef add_job_logs(job_id job_id, list<LogLine> lines)
        returns (int line_number) authentication required;
    /*
        skip_lines - optional parameter, number of lines to skip (in case they were
            already loaded before).
    */
    typedef structure {
        job_id job_id;
        int skip_lines;
    } GetJobLogsParams;
    /*
        last_line_number - common number of lines (including those in skip_lines
            parameter), this number can be used as next skip_lines value to
            skip already loaded lines next time.
    */
    typedef structure {
        list<LogLine> lines;
        int last_line_number;
    } GetJobLogsResults;
    funcdef get_job_logs(GetJobLogsParams params) returns (GetJobLogsResults)
        authentication required;
    /* Error block of JSON RPC response */
    typedef structure {
        string name;
        int code;
        string message;
        string error;
    } JsonRpcError;
    /*
        error_message: optional if job is finished with error
        job_output: job output if job completed successfully
    */
    typedef structure {
        job_id job_id;
        string error_message;
        UnspecifiedObject job_output;
    } FinishJobParams;
    /*
        Register results of already started job
    */
    funcdef finish_job(FinishJobParams params) returns () authentication required;

    /*
        skip_estimation: default true. If set true, job will set to running status skipping estimation step
    */
    typedef structure {
        job_id job_id;
        boolean skip_estimation;
    } StartJobParams;
    funcdef start_job(StartJobParams params) returns () authentication required;

    /*
        projection: projecct certain fields to return. default None.
    */
    typedef structure {
        job_id job_id;
        list<string> projection;
    } CheckJobParams;

    /*
        get current status of a job
    */
    funcdef check_job(CheckJobParams params) returns (UnspecifiedObject job_state) authentication required;

    /*
        job_states - states of jobs
    */
    typedef structure {
        mapping<job_id, UnspecifiedObject> job_states;
    } CheckJobsResults;

    typedef structure {
        list<job_id> job_ids;
        list<string> projection;
    } CheckJobsParams;

    funcdef check_jobs(CheckJobsParams params) returns (CheckJobsResults) authentication required;

    /*
      Check job for all jobs in a given workspace
    */
    typedef structure {
        string workspace_id;
        list<string> projection;
    } CheckWorkspaceJobsParams;

    funcdef check_workspace_jobs(CheckWorkspaceJobsParams params) returns (CheckJobsResults) authentication required;


    /*
      Check job for all jobs in a given date range for all users (Admin function)
    */
    typedef structure {
        string start_date;
        string end_date;
        list<string> projection;
    } CheckJobsDateRangeParams;

    funcdef check_jobs_date_range(CheckJobsDateRangeParams params) returns (CheckJobsResults) authentication required;

    /*
      Check job for all jobs in a given date range for a given user (Regular users can see their own jobs,
       admins can see other people's jobs)
    */
    typedef structure {
        string user;
        string start_date;
        string end_date;
        list<string> projection;
    } CheckJobsDateRangeForUserParams;

    funcdef check_jobs_date_range_for_user(CheckJobsDateRangeForUserParams params) returns (CheckJobsResults) authentication required;



    typedef structure {
        job_id job_id;
    } CancelJobParams;
    funcdef cancel_job(CancelJobParams params) returns () authentication required;
    /*
        job_id - id of job running method
        finished - indicates whether job is done (including error/cancel cases) or not
        canceled - whether the job is canceled or not.
        ujs_url - url of UserAndJobState service used by job service
    */
    typedef structure {
        job_id job_id;
        boolean finished;
        boolean canceled;
        string ujs_url;
    } CheckJobCanceledResult;
    /* Check whether a job has been canceled. This method is lightweight compared to check_job. */
    funcdef check_job_canceled(CancelJobParams params) returns (CheckJobCanceledResult result)
        authentication required;

    typedef structure {
        string status;
    } GetJobStatusResult;
    funcdef get_job_status(job_id job_id) returns (GetJobStatusResult result) authentication required;
};
