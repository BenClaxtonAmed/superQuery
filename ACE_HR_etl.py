import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ##################################################################################
# ############### BEGINNING OF MONKEY PATCH ########################################
# ##################################################################################
# Required for the monkey patch
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook, _DataflowJob
# We redefine the function that handles the environment keys 
# that are used to build the RuntimeEnvironment, to include 'ipConfiguration'
def _start_template_dataflow(self, name, variables, parameters,
                             dataflow_template):
    # Builds RuntimeEnvironment from variables dictionary
    # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
    environment = {}
    for key in ['numWorkers', 'maxWorkers', 'zone', 'serviceAccountEmail',
                'tempLocation', 'bypassTempDirValidation', 'machineType',
                'additionalExperiments', 'network', 'subnetwork', 'additionalUserLabels',
                'ipConfiguration']:
        if key in variables:
            environment.update({key: variables[key]})
    body = {"jobName": name,
            "parameters": parameters,
            "environment": environment}
    service = self.get_conn()
    request = service.projects().locations().templates().launch(
        projectId=variables['project'],
        location=variables['region'],
        gcsPath=dataflow_template,
        body=body
    )
    response = request.execute(num_retries=self.num_retries)
    variables = self._set_variables(variables)
    _DataflowJob(self.get_conn(), variables['project'], name, variables['region'],
                 self.poll_sleep, num_retries=self.num_retries).wait_for_done()
    return response
# Monkey patching
DataFlowHook._start_template_dataflow = _start_template_dataflow
# ##################################################################################
# ############### END OF MONKEY PATCH ##############################################
# ##################################################################################

bucket_path = models.Variable.get("bucket_path")
temp_location = bucket_path + "/tmp/"
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")
network = models.Variable.get("network")
subnetwork = models.Variable.get("subnetwork")
ipConfiguration = models.Variable.get("ipConfiguration")
template = models.Variable.get("template")
driverJars = models.Variable.get("driverJars")
driverClassName = models.Variable.get("driverClassName")
connectionURL = models.Variable.get("connectionURL")
connectionURL2 = models.Variable.get("connectionURL2")
bigQueryLoadingTemporaryDirectory = models.Variable.get("bigQueryLoadingTemporaryDirectory")
username = models.Variable.get("username")
password = models.Variable.get("password")

default_args = {
    "start_date": days_ago(1),
    "email": ["michael.helms@amedisys.com","benjamin.claxton@amedisys.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "dataflow_default_options": {
        "project": project_id, # "amed-dev-analyticsplatform"
        "region": gce_region, # "us-central1"
        "zone": gce_zone, # "us-central1-a"
        "temp_location": temp_location, # "gs://us-central1-amed-dev-compos-1b78d420-bucket/dags/tmp"
        "network": network, # "amedisys-shared-vpc"
        "subnetwork": subnetwork, # "https://www.googleapis.com/compute/v1/projects/amedisys-shared-services/regions/us-central1/subnetworks/amed-us-central1"
        "ipConfiguration": ipConfiguration # "WORKER_IP_PRIVATE"
    },
}

with models.DAG(
    "ACE_HR_etl",
    default_args=default_args,
    schedule_interval="06 22 * * *",  #UTC Minute Hour (5:05pm CDT = 05 22)
    concurrency=30
) as dag:

    hchba_dbo_agencies_truncate = BigQueryOperator(
        task_id="hchba_dbo_agencies_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_AGENCIES"
    )

    hchba_dbo_agencies_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_agencies_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_AGENCIES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_AGENCIES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_agencies_servicelines_branches_truncate = BigQueryOperator(
        task_id="hchba_dbo_agencies_servicelines_branches_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_AGENCIES_SERVICELINES_BRANCHES"
    )

    hchba_dbo_agencies_servicelines_branches_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_agencies_servicelines_branches_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_AGENCIES_SERVICELINES_BRANCHES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_AGENCIES_SERVICELINES_BRANCHES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_branches_truncate = BigQueryOperator(
        task_id="hchba_dbo_branches_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_BRANCHES"
    )

    hchba_dbo_branches_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_branches_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_BRANCHES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_BRANCHES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_job_descriptions_truncate = BigQueryOperator(
        task_id="hchba_dbo_job_descriptions_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_JOB_DESCRIPTIONS"
    )

    hchba_dbo_job_descriptions_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_job_descriptions_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_JOB_DESCRIPTIONS",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_JOB_DESCRIPTIONS",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_marital_statuses_truncate = BigQueryOperator(
        task_id="hchba_dbo_marital_statuses_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_MARITAL_STATUSES"
    )

    hchba_dbo_marital_statuses_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_marital_statuses_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_MARITAL_STATUSES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_MARITAL_STATUSES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_races_truncate = BigQueryOperator(
        task_id="hchba_dbo_races_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_RACES"
    )

    hchba_dbo_races_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_races_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_RACES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_RACES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_worker_base_truncate = BigQueryOperator(
        task_id="hchba_dbo_worker_base_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_WORKER_BASE"
    )

    hchba_dbo_worker_base_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_worker_base_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_WORKER_BASE",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_WORKER_BASE",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_worker_homebranch_truncate = BigQueryOperator(
        task_id="hchba_dbo_worker_homebranch_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_WORKER_HOMEBRANCH"
    )

    hchba_dbo_worker_homebranch_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_worker_homebranch_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_WORKER_HOMEBRANCH",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_WORKER_HOMEBRANCH",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_worker_statuses_truncate = BigQueryOperator(
        task_id="hchba_dbo_worker_statuses_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_WORKER_STATUSES"
    )

    hchba_dbo_worker_statuses_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_worker_statuses_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_WORKER_STATUSES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_WORKER_STATUSES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchba_dbo_worker_types_truncate = BigQueryOperator(
        task_id="hchba_dbo_worker_types_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_AMEDISYS_SC.dbo_WORKER_TYPES"
    )

    hchba_dbo_worker_types_etl = DataflowTemplateOperator(
        task_id="hchba_dbo_worker_types_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBA.dbo_WORKER_TYPES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_AMEDISYS_SC.dbo_WORKER_TYPES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    hchbi_dbo_branches_truncate = BigQueryOperator(
        task_id="hchbi_dbo_branches_truncate",
        use_legacy_sql=False,
        sql="truncate table HCHB_INFINITY_SC.dbo_BRANCHES"
    )

    hchbi_dbo_branches_etl = DataflowTemplateOperator(
        task_id="hchbi_dbo_branches_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL2,
            "query": "SELECT * FROM AceIntegration_GCP.HCHBI.dbo_BRANCHES",
            "outputTable": "amed-dev-analyticsplatform:HCHB_INFINITY_SC.dbo_BRANCHES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_acct_cd_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_acct_cd_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_ACCT_CD_TBL"
    )

    peoplesoft_dbo_ps_acct_cd_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_acct_cd_tbl_etl",
        template=template,
        priority_weight=1000,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_ACCT_CD_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_ACCT_CD_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_action_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_action_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_ACTION_TBL"
    )

    peoplesoft_dbo_ps_action_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_action_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_ACTION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_ACTION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_actn_reason_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_actn_reason_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_ACTN_REASON_TBL"
    )

    peoplesoft_dbo_ps_actn_reason_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_actn_reason_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_ACTN_REASON_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_ACTN_REASON_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_addresses_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_addresses_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_ADDRESSES"
    )

    peoplesoft_dbo_ps_addresses_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_addresses_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_ADDRESSES",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_ADDRESSES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_benef_plan_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_benef_plan_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_BENEF_PLAN_TBL"
    )

    peoplesoft_dbo_ps_benef_plan_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_benef_plan_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_BENEF_PLAN_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_BENEF_PLAN_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_bus_unit_tbl_fs_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_bus_unit_tbl_fs_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_BUS_UNIT_TBL_FS"
    )

    peoplesoft_dbo_ps_bus_unit_tbl_fs_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_bus_unit_tbl_fs_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_BUS_UNIT_TBL_FS",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_BUS_UNIT_TBL_FS",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_comp_ratecd_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_comp_ratecd_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_COMP_RATECD_TBL"
    )

    peoplesoft_dbo_ps_comp_ratecd_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_comp_ratecd_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_COMP_RATECD_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_COMP_RATECD_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_compensation_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_compensation_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_COMPENSATION"
    )

    peoplesoft_dbo_ps_compensation_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_compensation_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_COMPENSATION",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_COMPENSATION",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_deduction_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_deduction_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_DEDUCTION_TBL"
    )

    peoplesoft_dbo_ps_deduction_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_deduction_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_DEDUCTION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_DEDUCTION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_dept_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_dept_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_DEPT_TBL"
    )

    peoplesoft_dbo_ps_dept_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_dept_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_DEPT_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_DEPT_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_divers_ethnic_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_divers_ethnic_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_DIVERS_ETHNIC"
    )

    peoplesoft_dbo_ps_divers_ethnic_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_divers_ethnic_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_DIVERS_ETHNIC",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_DIVERS_ETHNIC",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_earnings_spcl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_earnings_spcl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_EARNINGS_SPCL"
    )

    peoplesoft_dbo_ps_earnings_spcl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_earnings_spcl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_EARNINGS_SPCL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_EARNINGS_SPCL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_earnings_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_earnings_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_EARNINGS_TBL"
    )

    peoplesoft_dbo_ps_earnings_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_earnings_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_EARNINGS_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_EARNINGS_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_email_addresses_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_email_addresses_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_EMAIL_ADDRESSES"
    )

    peoplesoft_dbo_ps_email_addresses_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_email_addresses_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_EMAIL_ADDRESSES",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_EMAIL_ADDRESSES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_employees_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_employees_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_EMPLOYEES"
    )

    peoplesoft_dbo_ps_employees_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_employees_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_EMPLOYEES",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_EMPLOYEES",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_ethnic_grp_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_ethnic_grp_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_ETHNIC_GRP_TBL"
    )

    peoplesoft_dbo_ps_ethnic_grp_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_ethnic_grp_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_ETHNIC_GRP_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_ETHNIC_GRP_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_job_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_job_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JOB"
    )

    peoplesoft_dbo_ps_job_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_job_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JOB",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JOB",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_job_family_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_job_family_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JOB_FAMILY_TBL"
    )

    peoplesoft_dbo_ps_job_family_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_job_family_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JOB_FAMILY_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JOB_FAMILY_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jobcd_comp_rate_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jobcd_comp_rate_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JOBCD_COMP_RATE"
    )

    peoplesoft_dbo_ps_jobcd_comp_rate_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jobcd_comp_rate_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JOBCD_COMP_RATE",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JOBCD_COMP_RATE",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jobcode_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jobcode_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JOBCODE_TBL"
    )

    peoplesoft_dbo_ps_jobcode_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jobcode_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JOBCODE_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JOBCODE_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jobfunction_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jobfunction_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JOBFUNCTION_TBL"
    )

    peoplesoft_dbo_ps_jobfunction_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jobfunction_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JOBFUNCTION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JOBFUNCTION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jpm_cat_items_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jpm_cat_items_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JPM_CAT_ITEMS"
    )

    peoplesoft_dbo_ps_jpm_cat_items_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jpm_cat_items_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JPM_CAT_ITEMS",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JPM_CAT_ITEMS",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jpm_jp_items_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jpm_jp_items_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JPM_JP_ITEMS"
    )

    peoplesoft_dbo_ps_jpm_jp_items_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jpm_jp_items_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JPM_JP_ITEMS",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JPM_JP_ITEMS",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_jpm_profile_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_jpm_profile_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_JPM_PROFILE"
    )

    peoplesoft_dbo_ps_jpm_profile_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_jpm_profile_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_JPM_PROFILE",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_JPM_PROFILE",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_leave_accrual_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_leave_accrual_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_LEAVE_ACCRUAL"
    )

    peoplesoft_dbo_ps_leave_accrual_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_leave_accrual_etl",
        template=template,
        priority_weight=1000,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_LEAVE_ACCRUAL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_LEAVE_ACCRUAL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_local_tax_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_local_tax_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_LOCAL_TAX_TBL"
    )

    peoplesoft_dbo_ps_local_tax_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_local_tax_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_LOCAL_TAX_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_LOCAL_TAX_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_location_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_location_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_LOCATION_TBL"
    )

    peoplesoft_dbo_ps_location_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_location_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_LOCATION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_LOCATION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    def xcom_pull_ps_pay_earnings_max(**kwargs):
          ti = kwargs['ti']
          bq_data = ti.xcom_pull(task_ids='get_data_ps_pay_earnings_max')
          # bq_data has the return value in a Python list, so convert to a string and remove brackets
          return(str(bq_data[0])[1:-1])

    peoplesoft_dbo_ps_pay_earnings_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_earnings_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAY_EARNINGS_STAGING"
    )

    pay_earnings_max_date = BigQueryOperator(
        task_id="pay_earnings_max_date",
        use_legacy_sql=False,
        sql="CREATE OR REPLACE TABLE HRPRD_SC.dbo_PS_PAY_EARNINGS_MAX AS SELECT CAST(COALESCE(MAX(PAY_END_DT),'1901-01-01') AS DATE) `PAY_END_DT` FROM HRPRD_SC.dbo_PS_PAY_EARNINGS;"
    )

    get_data_ps_pay_earnings_max = BigQueryGetDataOperator(
        task_id='get_data_ps_pay_earnings_max',
        dataset_id='HRPRD_SC',
        table_id='dbo_PS_PAY_EARNINGS_MAX',
        max_results=1,
        selected_fields='PAY_END_DT'
    )

    process_ps_pay_earnings_max = PythonOperator(
          task_id='process_ps_pay_earnings_max',
          python_callable=xcom_pull_ps_pay_earnings_max,
          provide_context=True
    )

    peoplesoft_dbo_ps_pay_earnings_stage = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pay_earnings_stage",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAY_EARNINGS WHERE PAY_END_DT >= {{ ti.xcom_pull(task_ids='process_ps_pay_earnings_max') }}",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAY_EARNINGS_STAGING",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pay_earnings_merge = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_earnings_merge",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.MergePayEarnings`();",
        priority_weight=1000
    )

    def xcom_pull_ps_pay_tax_max(**kwargs):
          ti = kwargs['ti']
          bq_data = ti.xcom_pull(task_ids='get_data_ps_pay_tax_max')
          # bq_data has the return value in a Python list, so convert to a string and remove brackets
          return(str(bq_data[0])[1:-1])

    peoplesoft_dbo_ps_pay_tax_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_tax_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAY_TAX_STAGING"
    )

    pay_tax_max_date = BigQueryOperator(
        task_id="pay_tax_max_date",
        use_legacy_sql=False,
        sql="CREATE OR REPLACE TABLE HRPRD_SC.dbo_PS_PAY_TAX_MAX AS SELECT CAST(COALESCE(MAX(PAY_END_DT),'1901-01-01') AS DATE) `PAY_END_DT` FROM HRPRD_SC.dbo_PS_PAY_TAX;"
    )

    get_data_ps_pay_tax_max = BigQueryGetDataOperator(
        task_id='get_data_ps_pay_tax_max',
        dataset_id='HRPRD_SC',
        table_id='dbo_PS_PAY_TAX_MAX',
        max_results=1,
        selected_fields='PAY_END_DT'
    )

    process_ps_pay_tax_max = PythonOperator(
          task_id='process_ps_pay_tax_max',
          python_callable=xcom_pull_ps_pay_tax_max,
          provide_context=True
    )

    peoplesoft_dbo_ps_pay_tax_stage = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pay_tax_stage",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAY_TAX WHERE PAY_END_DT >= {{ ti.xcom_pull(task_ids='process_ps_pay_tax_max') }}",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAY_TAX_STAGING",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pay_tax_merge = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_tax_merge",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.MergePayTax`();",
        priority_weight=1000
    )

    def xcom_pull_ps_pay_oth_earns_max(**kwargs):
          ti = kwargs['ti']
          bq_data = ti.xcom_pull(task_ids='get_data_ps_pay_oth_earns_max')
          # bq_data has the return value in a Python list, so convert to a string and remove brackets
          return(str(bq_data[0])[1:-1])

    peoplesoft_dbo_ps_pay_oth_earns_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_oth_earns_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAY_OTH_EARNS_STAGING"
    )

    pay_oth_earns_max_date = BigQueryOperator(
        task_id="pay_oth_earns_max_date",
        use_legacy_sql=False,
        sql="CREATE OR REPLACE TABLE HRPRD_SC.dbo_PS_PAY_OTH_EARNS_MAX AS SELECT CAST(COALESCE(MAX(PAY_END_DT),'1901-01-01') AS DATE) `PAY_END_DT` FROM HRPRD_SC.dbo_PS_PAY_OTH_EARNS;"
    )

    get_data_ps_pay_oth_earns_max = BigQueryGetDataOperator(
        task_id='get_data_ps_pay_oth_earns_max',
        dataset_id='HRPRD_SC',
        table_id='dbo_PS_PAY_OTH_EARNS_MAX',
        max_results=1,
        selected_fields='PAY_END_DT'
    )

    process_ps_pay_oth_earns_max = PythonOperator(
          task_id='process_ps_pay_oth_earns_max',
          python_callable=xcom_pull_ps_pay_oth_earns_max,
          provide_context=True
    )

    peoplesoft_dbo_ps_pay_oth_earns_stage = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pay_oth_earns_stage",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAY_OTH_EARNS WHERE PAY_END_DT >= {{ ti.xcom_pull(task_ids='process_ps_pay_oth_earns_max') }}",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAY_OTH_EARNS_STAGING",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pay_oth_earns_merge = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_oth_earns_merge",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.MergePayOthEarns`();",
        priority_weight=1000
    )

    def xcom_pull_ps_pay_deduction_max(**kwargs):
          ti = kwargs['ti']
          bq_data = ti.xcom_pull(task_ids='get_data_ps_pay_deduction_max')
          # bq_data has the return value in a Python list, so convert to a string and remove brackets
          return(str(bq_data[0])[1:-1])

    peoplesoft_dbo_ps_pay_deduction_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_deduction_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAY_DEDUCTION_STAGING"
    )

    pay_deduction_max_date = BigQueryOperator(
        task_id="pay_deduction_max_date",
        use_legacy_sql=False,
        sql="CREATE OR REPLACE TABLE HRPRD_SC.dbo_PS_PAY_DEDUCTION_MAX AS SELECT CAST(COALESCE(MAX(PAY_END_DT),'1901-01-01') AS DATE) `PAY_END_DT` FROM HRPRD_SC.dbo_PS_PAY_DEDUCTION;"
    )

    get_data_ps_pay_deduction_max = BigQueryGetDataOperator(
        task_id='get_data_ps_pay_deduction_max',
        dataset_id='HRPRD_SC',
        table_id='dbo_PS_PAY_DEDUCTION_MAX',
        max_results=1,
        selected_fields='PAY_END_DT'
    )

    process_ps_pay_deduction_max = PythonOperator(
          task_id='process_ps_pay_deduction_max',
          python_callable=xcom_pull_ps_pay_deduction_max,
          provide_context=True
    )

    peoplesoft_dbo_ps_pay_deduction_stage = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pay_deduction_stage",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAY_DEDUCTION WHERE PAY_END_DT >= {{ ti.xcom_pull(task_ids='process_ps_pay_deduction_max') }}",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAY_DEDUCTION_STAGING",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pay_deduction_merge = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_deduction_merge",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.MergePayDeduction`();",
        priority_weight=1000
    )

    def xcom_pull_ps_pay_check_max(**kwargs):
          ti = kwargs['ti']
          bq_data = ti.xcom_pull(task_ids='get_data_ps_pay_check_max')
          # bq_data has the return value in a Python list, so convert to a string and remove brackets
          return(str(bq_data[0])[1:-1])

    peoplesoft_dbo_ps_pay_check_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_check_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAY_CHECK_STAGING"
    )

    pay_check_max_date = BigQueryOperator(
        task_id="pay_check_max_date",
        use_legacy_sql=False,
        sql="CREATE OR REPLACE TABLE HRPRD_SC.dbo_PS_PAY_CHECK_MAX AS SELECT CAST(COALESCE(MAX(PAY_END_DT),'1901-01-01') AS DATE) `PAY_END_DT` FROM HRPRD_SC.dbo_PS_PAY_CHECK;"
    )

    get_data_ps_pay_check_max = BigQueryGetDataOperator(
        task_id='get_data_ps_pay_check_max',
        dataset_id='HRPRD_SC',
        table_id='dbo_PS_PAY_CHECK_MAX',
        max_results=1,
        selected_fields='PAY_END_DT'
    )

    process_ps_pay_check_max = PythonOperator(
          task_id='process_ps_pay_check_max',
          python_callable=xcom_pull_ps_pay_check_max,
          provide_context=True
    )

    peoplesoft_dbo_ps_pay_check_stage = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pay_check_stage",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAY_CHECK WHERE PAY_END_DT >= {{ ti.xcom_pull(task_ids='process_ps_pay_check_max') }}",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAY_CHECK_STAGING",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pay_check_merge = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pay_check_merge",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.MergePayCheck`();",
        priority_weight=1000
    )

    peoplesoft_dbo_ps_paygroup_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_paygroup_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PAYGROUP_TBL"
    )

    peoplesoft_dbo_ps_paygroup_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_paygroup_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PAYGROUP_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PAYGROUP_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_per_org_asgn_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_per_org_asgn_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PER_ORG_ASGN"
    )

    peoplesoft_dbo_ps_per_org_asgn_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_per_org_asgn_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PER_ORG_ASGN",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PER_ORG_ASGN",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pers_data_effdt_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pers_data_effdt_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PERS_DATA_EFFDT"
    )

    peoplesoft_dbo_ps_pers_data_effdt_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pers_data_effdt_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PERS_DATA_EFFDT",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PERS_DATA_EFFDT",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_pers_nid_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_pers_nid_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PERS_NID"
    )

    peoplesoft_dbo_ps_pers_nid_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_pers_nid_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PERS_NID",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PERS_NID",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_person_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_person_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PERSON"
    )

    peoplesoft_dbo_ps_person_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_person_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PERSON",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PERSON",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_person_name_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_person_name_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PERSON_NAME"
    )

    peoplesoft_dbo_ps_person_name_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_person_name_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PERSON_NAME",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PERSON_NAME",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_person_phone_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_person_phone_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_PERSON_PHONE"
    )

    peoplesoft_dbo_ps_person_phone_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_person_phone_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_PERSON_PHONE",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_PERSON_PHONE",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_state_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_state_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_STATE_TBL"
    )

    peoplesoft_dbo_ps_state_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_state_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_STATE_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_STATE_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_z_acq_cd_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_z_acq_cd_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_Z_ACQ_CD_TBL"
    )

    peoplesoft_dbo_ps_z_acq_cd_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_z_acq_cd_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_Z_ACQ_CD_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_Z_ACQ_CD_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_z_location_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_z_location_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_Z_LOCATION_TBL"
    )

    peoplesoft_dbo_ps_z_location_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_z_location_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_Z_LOCATION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_Z_LOCATION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_z_pod_cd_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_z_pod_cd_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_Z_POD_CD_TBL"
    )

    peoplesoft_dbo_ps_z_pod_cd_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_z_pod_cd_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_Z_POD_CD_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_Z_POD_CD_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_ps_z_region_tbl_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_ps_z_region_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PS_Z_REGION_TBL"
    )

    peoplesoft_dbo_ps_z_region_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_ps_z_region_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PS_Z_REGION_TBL",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PS_Z_REGION_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoft_dbo_psxlatitem_truncate = BigQueryOperator(
        task_id="peoplesoft_dbo_psxlatitem_truncate",
        use_legacy_sql=False,
        sql="truncate table HRPRD_SC.dbo_PSXLATITEM"
    )

    peoplesoft_dbo_psxlatitem_etl = DataflowTemplateOperator(
        task_id="peoplesoft_dbo_psxlatitem_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationPROD09_GCP.PeopleSoft.dbo_PSXLATITEM",
            "outputTable": "amed-dev-analyticsplatform:HRPRD_SC.dbo_PSXLATITEM",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoftfs_dbo_ps_z_agncy_int_tbl_truncate = BigQueryOperator(
        task_id="peoplesoftfs_dbo_ps_z_agncy_int_tbl_truncate",
        use_legacy_sql=False,
        sql="truncate table FSPRD_SC.dbo_PS_Z_AGNCY_INT_TBL"
    )

    peoplesoftfs_dbo_ps_z_agncy_int_tbl_etl = DataflowTemplateOperator(
        task_id="peoplesoftfs_dbo_ps_z_agncy_int_tbl_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationFN92_GCP.PeopleSoftFS.dbo_PS_Z_AGNCY_INT_TBL",
            "outputTable": "amed-dev-analyticsplatform:FSPRD_SC.dbo_PS_Z_AGNCY_INT_TBL",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    peoplesoftfs_dbo_xlattable_vw_truncate = BigQueryOperator(
        task_id="peoplesoftfs_dbo_xlattable_vw_truncate",
        use_legacy_sql=False,
        sql="truncate table FSPRD_SC.dbo_XLATTABLE_VW"
    )

    peoplesoftfs_dbo_xlattable_vw_etl = DataflowTemplateOperator(
        task_id="peoplesoftfs_dbo_xlattable_vw_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegrationFN92_GCP.PeopleSoftFS.dbo_XLATTABLE_VW",
            "outputTable": "amed-dev-analyticsplatform:FSPRD_SC.dbo_XLATTABLE_VW",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    agency_dbo_agency_truncate = BigQueryOperator(
        task_id="agency_dbo_agency_truncate",
        use_legacy_sql=False,
        sql="truncate table Agency_SC.dbo_Agency"
    )

    agency_dbo_agency_etl = DataflowTemplateOperator(
        task_id="agency_dbo_agency_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegration_PROD03_GCP.Agency.dbo_Agency",
            "outputTable": "amed-dev-analyticsplatform:Agency_SC.dbo_Agency",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    agency_dbo_standingtype_truncate = BigQueryOperator(
        task_id="agency_dbo_standingtype_truncate",
        use_legacy_sql=False,
        sql="truncate table Agency_SC.dbo_StandingType"
    )

    agency_dbo_standingtype_etl = DataflowTemplateOperator(
        task_id="agency_dbo_standingtype_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegration_PROD03_GCP.Agency.dbo_StandingType",
            "outputTable": "amed-dev-analyticsplatform:Agency_SC.dbo_StandingType",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    agency_dbo_status_truncate = BigQueryOperator(
        task_id="agency_dbo_status_truncate",
        use_legacy_sql=False,
        sql="truncate table Agency_SC.dbo_Status"
    )

    agency_dbo_status_etl = DataflowTemplateOperator(
        task_id="agency_dbo_status_etl",
        template=template,
        parameters={
            "driverJars": driverJars,
            "driverClassName": driverClassName,
            "connectionURL": connectionURL,
            "query": "SELECT * FROM AceIntegration_PROD03_GCP.Agency.dbo_Status",
            "outputTable": "amed-dev-analyticsplatform:Agency_SC.dbo_Status",
            "bigQueryLoadingTemporaryDirectory": bigQueryLoadingTemporaryDirectory,
            "username": username,
            "password": password
        }
    )

    loaddimemployee = BigQueryOperator(
        task_id="loaddimemployee",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimEmployee`();"
    )

    loaddimlocation = BigQueryOperator(
        task_id="loaddimlocation",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimLocation`();"
    )

    loaddimcertificate = BigQueryOperator(
        task_id="loaddimcertificate",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimCertificate`();"
    )

    loaddimjobactionreason = BigQueryOperator(
        task_id="loaddimjobactionreason",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimJobActionReason`();"
    )

    loaddimjobcode = BigQueryOperator(
        task_id="loaddimjobcode",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimJobCode`();"
    )

    loaddimjobjunk = BigQueryOperator(
        task_id="loaddimjobjunk",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimJobJunk`();"
    )

    loaddimdepartment = BigQueryOperator(
        task_id="loaddimdepartment",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimDepartment`();"
    )

    loadfactcertificateexpiration = BigQueryOperator(
        task_id="loadfactcertificateexpiration",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactCertificateExpiration`();"
    )

    loadfactjobaction = BigQueryOperator(
        task_id="loadfactjobaction",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactJobAction`();"
    )

    loadfactjobfamily = BigQueryOperator(
        task_id="loadfactjobfamily",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactJobFamily`();"
    )

    loadfactterminationtype = BigQueryOperator(
        task_id="loadfactterminationtype",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactTerminationType`();"
    )

    loadpaycheck = BigQueryOperator(
        task_id="loadpaycheck",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadPaycheck`();"
    )

    loadpaycheckearnings = BigQueryOperator(
        task_id="loadpaycheckearnings",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadPaycheckEarnings`();"
    )

    loadpaychecktax = BigQueryOperator(
        task_id="loadpaychecktax",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadPaycheckTax`();"
    )

    loademployeecompensation = BigQueryOperator(
        task_id="loademployeecompensation",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadEmployeeCompensation`();"
    )

    loadfactpaychecktax = BigQueryOperator(
        task_id="loadfactpaychecktax",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactPaycheckTax`();"
    )

    loadfactpaychecksummary = BigQueryOperator(
        task_id="loadfactpaychecksummary",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactPaycheckSummary`();"
    )

    loadfactpaycheckearnings = BigQueryOperator(
        task_id="loadfactpaycheckearnings",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactPaycheckEarnings`();"
    )

    loadfactpaycheckdeduction = BigQueryOperator(
        task_id="loadfactpaycheckdeduction",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactPaycheckDeduction`();"
    )

    loadfactemployeecompensation = BigQueryOperator(
        task_id="loadfactemployeecompensation",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactEmployeeCompensation`();"
    )

    loaddimtaxclass = BigQueryOperator(
        task_id="loaddimtaxclass",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimTaxClass`();"
    )

    loaddimtaxauthority = BigQueryOperator(
        task_id="loaddimtaxauthority",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimTaxAuthority`();"
    )

    loaddimpaycheck = BigQueryOperator(
        task_id="loaddimpaycheck",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimPaycheck`();"
    )

    loaddimearningstype = BigQueryOperator(
        task_id="loaddimearningstype",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimEarningsType`();"
    )

    loaddimdeductiontype = BigQueryOperator(
        task_id="loaddimdeductiontype",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimDeductionType`();"
    )

    loaddimdeductionclass = BigQueryOperator(
        task_id="loaddimdeductionclass",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimDeductionClass`();"
    )

    loaddimcompensationratetype = BigQueryOperator(
        task_id="loaddimcompensationratetype",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimCompensationRateType`();"
    )

    loaddimbenefitplan = BigQueryOperator(
        task_id="loaddimbenefitplan",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimBenefitPlan`();"
    )

    loadfactemployeecensusmonthly = BigQueryOperator(
        task_id="loadfactemployeecensusmonthly",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadFactEmployeeCensusMonthly`();"
    )

    loaddimmonth = BigQueryOperator(
        task_id="loaddimmonth",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimMonth`();"
    )

    loaddimjobcodehistoric = BigQueryOperator(
        task_id="loaddimjobcodehistoric",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimJobCodeHistoric`();"
    )

    loaddimdateincremental = BigQueryOperator(
        task_id="loaddimdateincremental",
        use_legacy_sql=False,
        sql="CALL `amed-dev-analyticsplatform.ETLConfigACE.LoadDimDateIncremental`();"
    )

# --------------------- Source Table Loads (Agency) ---------------------------
agency_dbo_agency_truncate >> agency_dbo_agency_etl
agency_dbo_standingtype_truncate >> agency_dbo_standingtype_etl
agency_dbo_status_truncate >> agency_dbo_status_etl

# --------------------- Source Table Loads (HCHBA) ---------------------------
hchba_dbo_agencies_truncate >> hchba_dbo_agencies_etl
hchba_dbo_agencies_servicelines_branches_truncate >> hchba_dbo_agencies_servicelines_branches_etl
hchba_dbo_branches_truncate >> hchba_dbo_branches_etl
hchba_dbo_job_descriptions_truncate >> hchba_dbo_job_descriptions_etl
hchba_dbo_marital_statuses_truncate >> hchba_dbo_marital_statuses_etl
hchba_dbo_races_truncate >> hchba_dbo_races_etl
hchba_dbo_worker_base_truncate >> hchba_dbo_worker_base_etl
hchba_dbo_worker_homebranch_truncate >> hchba_dbo_worker_homebranch_etl
hchba_dbo_worker_statuses_truncate >> hchba_dbo_worker_statuses_etl
hchba_dbo_worker_types_truncate >> hchba_dbo_worker_types_etl

# --------------------- Source Table Loads (HCHBI) ---------------------------
hchbi_dbo_branches_truncate >> hchbi_dbo_branches_etl

# --------------------- Source Table Loads (HRPRD) ---------------------------
peoplesoft_dbo_ps_acct_cd_tbl_truncate >> peoplesoft_dbo_ps_acct_cd_tbl_etl
peoplesoft_dbo_ps_action_tbl_truncate >> peoplesoft_dbo_ps_action_tbl_etl
peoplesoft_dbo_ps_actn_reason_tbl_truncate >> peoplesoft_dbo_ps_actn_reason_tbl_etl
peoplesoft_dbo_ps_addresses_truncate >> peoplesoft_dbo_ps_addresses_etl
peoplesoft_dbo_ps_benef_plan_tbl_truncate >> peoplesoft_dbo_ps_benef_plan_tbl_etl
peoplesoft_dbo_ps_bus_unit_tbl_fs_truncate >> peoplesoft_dbo_ps_bus_unit_tbl_fs_etl
peoplesoft_dbo_ps_comp_ratecd_tbl_truncate >> peoplesoft_dbo_ps_comp_ratecd_tbl_etl
peoplesoft_dbo_ps_compensation_truncate >> peoplesoft_dbo_ps_compensation_etl
peoplesoft_dbo_ps_deduction_tbl_truncate >> peoplesoft_dbo_ps_deduction_tbl_etl
peoplesoft_dbo_ps_dept_tbl_truncate >> peoplesoft_dbo_ps_dept_tbl_etl
peoplesoft_dbo_ps_divers_ethnic_truncate >> peoplesoft_dbo_ps_divers_ethnic_etl
peoplesoft_dbo_ps_earnings_spcl_truncate >> peoplesoft_dbo_ps_earnings_spcl_etl
peoplesoft_dbo_ps_earnings_tbl_truncate >> peoplesoft_dbo_ps_earnings_tbl_etl
peoplesoft_dbo_ps_email_addresses_truncate >> peoplesoft_dbo_ps_email_addresses_etl
peoplesoft_dbo_ps_employees_truncate >> peoplesoft_dbo_ps_employees_etl
peoplesoft_dbo_ps_ethnic_grp_tbl_truncate >> peoplesoft_dbo_ps_ethnic_grp_tbl_etl
peoplesoft_dbo_ps_job_truncate >> peoplesoft_dbo_ps_job_etl
peoplesoft_dbo_ps_job_family_tbl_truncate >> peoplesoft_dbo_ps_job_family_tbl_etl
peoplesoft_dbo_ps_jobcd_comp_rate_truncate >> peoplesoft_dbo_ps_jobcd_comp_rate_etl
peoplesoft_dbo_ps_jobcode_tbl_truncate >> peoplesoft_dbo_ps_jobcode_tbl_etl
peoplesoft_dbo_ps_jobfunction_tbl_truncate >> peoplesoft_dbo_ps_jobfunction_tbl_etl
peoplesoft_dbo_ps_jpm_cat_items_truncate >> peoplesoft_dbo_ps_jpm_cat_items_etl
peoplesoft_dbo_ps_jpm_jp_items_truncate >> peoplesoft_dbo_ps_jpm_jp_items_etl
peoplesoft_dbo_ps_jpm_profile_truncate >> peoplesoft_dbo_ps_jpm_profile_etl
peoplesoft_dbo_ps_leave_accrual_truncate >> peoplesoft_dbo_ps_leave_accrual_etl
peoplesoft_dbo_ps_local_tax_tbl_truncate >> peoplesoft_dbo_ps_local_tax_tbl_etl
peoplesoft_dbo_ps_location_tbl_truncate >> peoplesoft_dbo_ps_location_tbl_etl
peoplesoft_dbo_ps_paygroup_tbl_truncate >> peoplesoft_dbo_ps_paygroup_tbl_etl
peoplesoft_dbo_ps_per_org_asgn_truncate >> peoplesoft_dbo_ps_per_org_asgn_etl
peoplesoft_dbo_ps_pers_data_effdt_truncate >> peoplesoft_dbo_ps_pers_data_effdt_etl
peoplesoft_dbo_ps_pers_nid_truncate >> peoplesoft_dbo_ps_pers_nid_etl
peoplesoft_dbo_ps_person_truncate >> peoplesoft_dbo_ps_person_etl
peoplesoft_dbo_ps_person_name_truncate >> peoplesoft_dbo_ps_person_name_etl
peoplesoft_dbo_ps_person_phone_truncate >> peoplesoft_dbo_ps_person_phone_etl
peoplesoft_dbo_ps_state_tbl_truncate >> peoplesoft_dbo_ps_state_tbl_etl
peoplesoft_dbo_ps_z_acq_cd_tbl_truncate >> peoplesoft_dbo_ps_z_acq_cd_tbl_etl
peoplesoft_dbo_ps_z_location_tbl_truncate >> peoplesoft_dbo_ps_z_location_tbl_etl
peoplesoft_dbo_ps_z_pod_cd_tbl_truncate >> peoplesoft_dbo_ps_z_pod_cd_tbl_etl
peoplesoft_dbo_ps_z_region_tbl_truncate >> peoplesoft_dbo_ps_z_region_tbl_etl
peoplesoft_dbo_psxlatitem_truncate >> peoplesoft_dbo_psxlatitem_etl
peoplesoft_dbo_ps_pay_check_truncate >> pay_check_max_date >> get_data_ps_pay_check_max >> process_ps_pay_check_max >> peoplesoft_dbo_ps_pay_check_stage >> peoplesoft_dbo_ps_pay_check_merge
peoplesoft_dbo_ps_pay_tax_truncate >> pay_tax_max_date >> get_data_ps_pay_tax_max >> process_ps_pay_tax_max >> peoplesoft_dbo_ps_pay_tax_stage >> peoplesoft_dbo_ps_pay_tax_merge
peoplesoft_dbo_ps_pay_earnings_truncate >> pay_earnings_max_date >> get_data_ps_pay_earnings_max >> process_ps_pay_earnings_max >> peoplesoft_dbo_ps_pay_earnings_stage >> peoplesoft_dbo_ps_pay_earnings_merge
peoplesoft_dbo_ps_pay_oth_earns_truncate >> pay_oth_earns_max_date >> get_data_ps_pay_oth_earns_max >> process_ps_pay_oth_earns_max >> peoplesoft_dbo_ps_pay_oth_earns_stage >> peoplesoft_dbo_ps_pay_oth_earns_merge
peoplesoft_dbo_ps_pay_deduction_truncate >> pay_deduction_max_date >> get_data_ps_pay_deduction_max >> process_ps_pay_deduction_max >> peoplesoft_dbo_ps_pay_deduction_stage >> peoplesoft_dbo_ps_pay_deduction_merge

# --------------------- Source Table Loads (FSPRD) ---------------------------
peoplesoftfs_dbo_ps_z_agncy_int_tbl_truncate >> peoplesoftfs_dbo_ps_z_agncy_int_tbl_etl
peoplesoftfs_dbo_xlattable_vw_truncate >> peoplesoftfs_dbo_xlattable_vw_etl

# --------------------- LoadDimBenefitPlan Transform -----------------------
peoplesoft_dbo_ps_benef_plan_tbl_etl >> loaddimbenefitplan
peoplesoft_dbo_psxlatitem_etl >> loaddimbenefitplan

# --------------------- LoadDimCertificate Transform -----------------------
peoplesoft_dbo_ps_jpm_cat_items_etl >> loaddimcertificate

# --------------------- LoadDimCompensationRateType Transform -----------------------
loademployeecompensation >> loaddimcompensationratetype

# --------------------- LoadDimDeductionClass Transform -----------------------
peoplesoft_dbo_psxlatitem_etl >> loaddimdeductionclass

# --------------------- LoadDimDeductionType Transform -----------------------
peoplesoft_dbo_ps_deduction_tbl_etl >> loaddimdeductiontype

# --------------------- LoadDimDepartment Transform -----------------------
peoplesoft_dbo_ps_dept_tbl_etl >> loaddimdepartment

# --------------------- LoadDimEarningsType Transform -----------------------
loadpaycheckearnings >> loaddimearningstype
peoplesoft_dbo_ps_earnings_spcl_etl >> loaddimearningstype

# --------------------- LoadDimEmployee Transform -----------------------
hchba_dbo_job_descriptions_etl >> loaddimemployee
hchba_dbo_marital_statuses_etl >> loaddimemployee
hchba_dbo_races_etl >> loaddimemployee
hchba_dbo_worker_base_etl >> loaddimemployee
hchba_dbo_worker_homebranch_etl >> loaddimemployee
hchba_dbo_worker_statuses_etl >> loaddimemployee
hchba_dbo_worker_types_etl >> loaddimemployee
loaddimlocation >> loaddimemployee
peoplesoft_dbo_ps_addresses_etl >> loaddimemployee
peoplesoft_dbo_ps_dept_tbl_etl >> loaddimemployee
peoplesoft_dbo_ps_divers_ethnic_etl >> loaddimemployee
peoplesoft_dbo_ps_email_addresses_etl >> loaddimemployee
peoplesoft_dbo_ps_employees_etl >> loaddimemployee
peoplesoft_dbo_ps_ethnic_grp_tbl_etl >> loaddimemployee
peoplesoft_dbo_ps_job_etl >> loaddimemployee
peoplesoft_dbo_ps_jobcode_tbl_etl >> loaddimemployee
peoplesoft_dbo_ps_location_tbl_etl >> loaddimemployee
peoplesoft_dbo_ps_per_org_asgn_etl >> loaddimemployee
peoplesoft_dbo_ps_pers_data_effdt_etl >> loaddimemployee
peoplesoft_dbo_ps_pers_nid_etl >> loaddimemployee
peoplesoft_dbo_ps_person_etl >> loaddimemployee
peoplesoft_dbo_ps_person_name_etl >> loaddimemployee
peoplesoft_dbo_ps_person_phone_etl >> loaddimemployee
peoplesoftfs_dbo_xlattable_vw_etl >> loaddimemployee

# --------------------- LoadDimJobActionReason Transform -----------------------
peoplesoft_dbo_ps_action_tbl_etl >> loaddimjobactionreason
peoplesoft_dbo_ps_actn_reason_tbl_etl >> loaddimjobactionreason

# --------------------- LoadDimJobCode Transform -----------------------
peoplesoft_dbo_ps_job_family_tbl_etl >> loaddimjobcode
peoplesoft_dbo_ps_jobcode_tbl_etl >> loaddimjobcode
peoplesoft_dbo_ps_jobfunction_tbl_etl >> loaddimjobcode

# --------------------- LoadDimJobCodeHistoric Transform -----------------------
peoplesoft_dbo_ps_job_family_tbl_etl >> loaddimjobcodehistoric
peoplesoft_dbo_ps_jobcode_tbl_etl >> loaddimjobcodehistoric

# --------------------- LoadDimJobJunk Transform -----------------------
peoplesoft_dbo_ps_job_etl >> loaddimjobjunk
peoplesoft_dbo_ps_paygroup_tbl_etl >> loaddimjobjunk
peoplesoft_dbo_ps_z_acq_cd_tbl_etl >> loaddimjobjunk
peoplesoft_dbo_psxlatitem_etl >> loaddimjobjunk

# --------------------- LoadDimLocation Transform -----------------------
hchba_dbo_agencies_etl >> loaddimlocation
hchba_dbo_agencies_servicelines_branches_etl >> loaddimlocation
hchba_dbo_branches_etl >> loaddimlocation
hchbi_dbo_branches_etl >> loaddimlocation
peoplesoft_dbo_ps_bus_unit_tbl_fs_etl >> loaddimlocation
peoplesoft_dbo_ps_email_addresses_etl >> loaddimlocation
peoplesoft_dbo_ps_location_tbl_etl >> loaddimlocation
peoplesoft_dbo_ps_person_name_etl >> loaddimlocation
peoplesoft_dbo_ps_z_acq_cd_tbl_etl >> loaddimlocation
peoplesoft_dbo_ps_z_location_tbl_etl >> loaddimlocation
peoplesoft_dbo_ps_z_pod_cd_tbl_etl >> loaddimlocation
peoplesoft_dbo_ps_z_region_tbl_etl >> loaddimlocation
peoplesoftfs_dbo_ps_z_agncy_int_tbl_etl >> loaddimlocation
agency_dbo_agency_etl >> loaddimlocation
agency_dbo_standingtype_etl >> loaddimlocation
agency_dbo_status_etl >> loaddimlocation

# --------------------- LoadDimMonth Transform -----------------------
loaddimdateincremental >> loaddimmonth

# --------------------- LoadDimPaycheck Transform -----------------------
loadpaycheck >> loaddimpaycheck

# --------------------- LoadDimTaxAuthority Transform -----------------------
loadpaychecktax >> loaddimtaxauthority

# --------------------- LoadDimTaxClass Transform -----------------------
loadpaychecktax >> loaddimtaxclass

# --------------------- LoadEmployeeCompensation Transform -----------------------
peoplesoft_dbo_ps_comp_ratecd_tbl_etl >> loademployeecompensation
peoplesoft_dbo_ps_compensation_etl >> loademployeecompensation
peoplesoft_dbo_ps_job_etl >> loademployeecompensation
peoplesoft_dbo_ps_jobcd_comp_rate_etl >> loademployeecompensation
peoplesoft_dbo_ps_z_location_tbl_etl >> loademployeecompensation
peoplesoft_dbo_psxlatitem_etl >> loademployeecompensation

# --------------------- LoadFactCertificateExpiration Transform -----------------------
loaddimcertificate >> loadfactcertificateexpiration
loaddimemployee >> loadfactcertificateexpiration
loaddimlocation >> loadfactcertificateexpiration
peoplesoft_dbo_ps_job_etl >> loadfactcertificateexpiration
peoplesoft_dbo_ps_jpm_cat_items_etl >> loadfactcertificateexpiration
peoplesoft_dbo_ps_jpm_jp_items_etl >> loadfactcertificateexpiration
peoplesoft_dbo_ps_jpm_profile_etl >> loadfactcertificateexpiration

# --------------------- LoadFactEmployeeCensusMonthly Transform -----------------------
loaddimemployee >> loadfactemployeecensusmonthly
loaddimjobactionreason >> loadfactemployeecensusmonthly
loaddimjobcode >> loadfactemployeecensusmonthly
loaddimjobcodehistoric >> loadfactemployeecensusmonthly
loaddimmonth >> loadfactemployeecensusmonthly
loadfactjobaction >> loadfactemployeecensusmonthly

# --------------------- LoadFactEmployeeCompensation Transform -----------------------
loaddimcompensationratetype >> loadfactemployeecompensation
loaddimemployee >> loadfactemployeecompensation
loaddimjobcode >> loadfactemployeecompensation
loaddimlocation >> loadfactemployeecompensation
loademployeecompensation >> loadfactemployeecompensation

# --------------------- LoadFactJobAction Transform -----------------------
loaddimdateincremental >> loadfactjobaction
loaddimdepartment >> loadfactjobaction
loaddimemployee >> loadfactjobaction
loaddimjobactionreason >> loadfactjobaction
loaddimjobcode >> loadfactjobaction
loaddimjobjunk >> loadfactjobaction
loaddimlocation >> loadfactjobaction
peoplesoft_dbo_ps_job_etl >> loadfactjobaction
peoplesoft_dbo_ps_per_org_asgn_etl >> loadfactjobaction
peoplesoft_dbo_ps_z_acq_cd_tbl_etl >> loadfactjobaction

# --------------------- LoadFactJobFamily Transform -----------------------
loaddimdateincremental >> loadfactjobfamily
loaddimjobcode >> loadfactjobfamily
peoplesoft_dbo_ps_job_family_tbl_etl >> loadfactjobfamily
peoplesoft_dbo_ps_jobcode_tbl_etl >> loadfactjobfamily

# --------------------- LoadFactPaycheckDeduction Transform -----------------------
loaddimbenefitplan >> loadfactpaycheckdeduction
loaddimdeductionclass >> loadfactpaycheckdeduction
loaddimdeductiontype >> loadfactpaycheckdeduction
loaddimpaycheck >> loadfactpaycheckdeduction
loadfactpaychecksummary >> loadfactpaycheckdeduction
peoplesoft_dbo_ps_pay_deduction_merge >> loadfactpaycheckdeduction

# --------------------- LoadFactPaycheckEarnings Transform -----------------------
loaddimdepartment >> loadfactpaycheckearnings
loaddimearningstype >> loadfactpaycheckearnings
loaddimjobcode >> loadfactpaycheckearnings
loaddimlocation >> loadfactpaycheckearnings
loaddimpaycheck >> loadfactpaycheckearnings
loadfactpaychecksummary >> loadfactpaycheckearnings
loadpaycheckearnings >> loadfactpaycheckearnings

# --------------------- LoadFactPaycheckSummary Transform -----------------------
loaddimemployee >> loadfactpaychecksummary
loaddimjobcode >> loadfactpaychecksummary
loaddimlocation >> loadfactpaychecksummary
loadpaycheck >> loadfactpaychecksummary

# --------------------- LoadFactPaycheckTax Transform -----------------------
loaddimpaycheck >> loadfactpaychecktax
loaddimtaxauthority >> loadfactpaychecktax
loaddimtaxclass >> loadfactpaychecktax
loadfactpaychecksummary >> loadfactpaychecktax
loadpaychecktax >> loadfactpaychecktax

# --------------------- LoadFactTerminationType Transform -----------------------
loaddimdateincremental >> loadfactterminationtype
loaddimjobactionreason >> loadfactterminationtype
peoplesoft_dbo_ps_action_tbl_etl >> loadfactterminationtype
peoplesoft_dbo_ps_actn_reason_tbl_etl >> loadfactterminationtype

# --------------------- LoadPaycheck Transform -----------------------
peoplesoft_dbo_ps_job_etl >> loadpaycheck
peoplesoft_dbo_ps_leave_accrual_etl >> loadpaycheck
peoplesoft_dbo_ps_pay_check_merge >> loadpaycheck
peoplesoft_dbo_ps_paygroup_tbl_etl >> loadpaycheck
peoplesoft_dbo_psxlatitem_etl >> loadpaycheck

# --------------------- LoadPaycheckEarnings Transform -----------------------
peoplesoft_dbo_ps_acct_cd_tbl_etl >> loadpaycheckearnings
peoplesoft_dbo_ps_earnings_tbl_etl >> loadpaycheckearnings
peoplesoft_dbo_ps_pay_earnings_merge >> loadpaycheckearnings
peoplesoft_dbo_ps_pay_oth_earns_merge >> loadpaycheckearnings

# --------------------- LoadPaycheckTax Transform -----------------------
peoplesoft_dbo_ps_local_tax_tbl_etl >> loadpaychecktax
peoplesoft_dbo_ps_pay_tax_merge >> loadpaychecktax
peoplesoft_dbo_ps_state_tbl_etl >> loadpaychecktax
peoplesoft_dbo_psxlatitem_etl >> loadpaychecktax
