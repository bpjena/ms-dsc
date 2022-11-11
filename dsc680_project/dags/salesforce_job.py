from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.utils.email import send_email_smtp
from airflow.contrib.operators.ecs_operator import ECSOperator

import boto3

ssm = boto3.client('ssm')


def task_failure_callback(context):
    outer_task_failure_callback(context, email='binay.bapu@gmail.com')


def outer_task_failure_callback(context, email):
    subject = "[DEng-MWAA-Alert] Failed DAG: {0} | {1} ".format(
        context['task_instance_key_str'].split('__')[0],
        datetime.now()
    )
    html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Failed on: {2}""".format(
        context['task_instance_key_str'].split('__')[0],
        context['task_instance_key_str'].split('__')[1],
        datetime.now()
    )
    send_email_smtp(email, subject, html_content)


default_args = {
    'owner': 'Data-Eng',
    'depends_on_past': False,
    'email': ['binay.bapu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': task_failure_callback
}

REDSHIFT_USERNAME = Variable.get("deng-mwaa-user-redshift-secret")
REDSHIFT_PASSWORD = Variable.get("deng-mwaa-password-redshift-secret")

dag = DAG(dag_id="salesforce_redshift_transforms",
          description="SFDC api -> S3 -> Redshift -> Transforms on Redshift",
          schedule_interval='45 6 * * *',
          start_date=datetime(2020, 1, 1),
          default_args=default_args,
          max_active_runs=1,
          dagrun_timeout=timedelta(minutes=239),
          catchup=False,
          tags=['SFDC'])

# Get ECS configuration from SSM parameters
ecs_cluster = str(ssm.get_parameter(Name='/mwaa/ecs/cluster', WithDecryption=True)['Parameter']['Value'])
ecs_task_definition = str(ssm.get_parameter(Name='/mwaa/ecs/task_definition', WithDecryption=True)['Parameter']['Value'])
ecs_subnets = str(ssm.get_parameter(Name='/mwaa/vpc/private_subnets', WithDecryption=True)['Parameter']['Value'])
ecs_security_group = str(ssm.get_parameter(Name='/mwaa/vpc/security_group', WithDecryption=True)['Parameter']['Value'])
ecs_awslogs_group = str(ssm.get_parameter(Name='/mwaa/cw/log_group', WithDecryption=True)['Parameter']['Value'])
ecs_awslogs_stream_prefix = str(ssm.get_parameter(Name='/mwaa/cw/log_stream', WithDecryption=True)['Parameter']['Value'])

# Run Docker container via ECS operator
sfdc_redshift_transforms = ECSOperator(
    task_id="redshift_sfdc_transforms",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "deng-jobs",
                "command": ["python", "/app/python/redshift/sfdc_transforms.py"],
                "environment": [
                    {
                        "name": "REDSHIFT_USERNAME",
                        "value": REDSHIFT_USERNAME
                    },
                    {
                        "name": "REDSHIFT_PASSWORD",
                        "value": REDSHIFT_PASSWORD
                    }
                ]
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": [ecs_security_group],
            "subnets": ecs_subnets.split(","),
        },
    },
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)


sfdc_redshift_transforms