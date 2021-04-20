
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import time
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

azk_flow = {
  "project" : "azkaban-test-project",
  "nodes" : [ {
    "id" : "test-final",
    "type" : "command",
    "in" : [ "test-job-4" ]
  },
      {
    "id" : "test-job-start",
    "type" : "java"
  }, {
    "id" : "test-job-3",
    "type" : "java",
    "in" : [ "test-job-2" ]
  }, {
    "id" : "test-job-4",
    "type" : "java",
    "in" : [ "test-job-3" ]
  }, {
    "id" : "test-job-2",
    "type" : "java",
    "in" : [ "test-job-start" ]
  } ],
  "flow" : "test",
  "projectId" : 193
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'glue_job_dag',
    default_args=default_args,
    description='PoC of glue job DAG translated from Azkaban workflow json',
    schedule_interval=timedelta(days=1),
)

def get_glue_operator_args(job_name, script_s3_path = None, worker_number = 5):
    glue_client = AwsBaseHook(aws_conn_id='aws_default', client_type='glue').get_client_type(client_type='glue',
                                                                                             region_name='us-east-2')
    response = glue_client.start_job_run(JobName=job_name)
    job_id = response['JobRunId']
    print("Job {} ID: {}".format(job_name,job_id))
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=job_id)
        state = status['JobRun']['JobRunState']
        if state == 'SUCCEEDED':
            print('Glue job {} run ID {} succeeded'.format(job_name,job_id))
            break
        if state in ['STOPPED', 'FAILED', 'TIMEOUT', 'STOPPING']:
            print('Glue job {} run ID {} is in {} state'.format(job_name,job_id, state))
            raise Exception
        time.sleep(20)

dag.doc_md = __doc__
glue_tasks = dict()
# operator over the existing Glue jobs
for node in azk_flow['nodes']:
    job_name = node['id']
    glue_tasks[job_name] = PythonOperator(
        task_id=job_name,
        python_callable=get_glue_operator_args,
        op_kwargs={'job_name':job_name},
        dag=dag
    )

# DAG structure is created by setting the upstream for all jobs
for node in azk_flow['nodes']:
    job_name = node['id']
    if 'in' in node:
        firing_job = node['in']
        glue_tasks[job_name].set_upstream(glue_tasks[firing_job])
