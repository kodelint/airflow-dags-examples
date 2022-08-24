from airflow import DAG
import os
from datetime import timedelta
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago


################################################################################
# Variables which can be changes as required                                   #
# EMR_NAME and DAG_ID get automatically populated based on the dag filename    #
################################################################################
EMR_NAME = DAG_ID = os.path.basename(__file__).replace(".py", "")

################################################################################
# Variables which can be changes as required                                   #
# EMR_VERSION, CUSTOM_AMI, DRIVER_MEMORY, EXECUTOR_MEMORY, EMR_LOG_URI         #
# NUMBER_OF_EXECUTOR, MASTER_INSTANCE_COUNT, MASTER_INSTANCE_TYPE              #
# CORE_INSTANCE_COUNT, CORE_INSTANCE_TYPE can be change and managed here       #
################################################################################
EMR_CLUSTER_REQUIREMENTS = {
    "EMR_VERSION": "emr-6.3.1",
    "CUSTOM_AMI": "ami-XXXXXXXXXXXXX",
    "DRIVER_MEMORY": "240g",
    "EXECUTOR_MEMORY": "70g",
    "NUMBER_OF_EXECUTOR": "200",
    "MASTER_INSTANCE_COUNT": 1,
    "MASTER_INSTANCE_TYPE": "i3.8xlarge",
    "CORE_INSTANCE_COUNT": 19,
    "CORE_INSTANCE_TYPE": "i3.8xlarge",
    "EMR_LOG_URI": "s3://some-s3-bucket/emr-logs/"
}
#################################################################################
# These are default arguments which can also be changed as needed               #
#################################################################################
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow-admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

#################################################################################
# These are core `spark-submit` command's arguments, which shouldn't be touched #
# until absolutely sure what are we trying to change                            #
#################################################################################
SPARK_SUBMIT_ARGUMENTS = [
    "spark-submit",
    "--deploy-mode", "client",
    "--driver-memory", EMR_CLUSTER_REQUIREMENTS["DRIVER_MEMORY"],
    "--executor-memory", EMR_CLUSTER_REQUIREMENTS["EXECUTOR_MEMORY"],
    "--num-executors", EMR_CLUSTER_REQUIREMENTS["NUMBER_OF_EXECUTOR"]
]

#################################################################################
# These are client spark job arguments which should be changes as needed        #
# please note that `EmrAddStepsOperator` take jobs arguments as list of strings #
# so, keep them in list of string in exact order you would run them locally     #
#################################################################################
SPARK_JOB_ARGUMENTS = [
    "--class", "com.example",
    "s3://some-s3-bucket/jars/example.jar",
    "-b", "s3://some-s3-bucket/output",
    "-u", Variable.get("DEV_USERNAME"),
    "-p", Variable.get("DEV_PASSWORD")
]

#################################################################################
# EMR Related settings, Subnets, Security groups, Additional Security Groups    #
# SSHKey, IAM Roles, Termination Protection, KeepAlive, Applications etc        #
# Should be handled here with map type variable `EMR_INFRA_SETTING`             #
#################################################################################
EMR_INFRA_SETTING = {
    "KeepJobFlowAliveWhenNoSteps": False,
    "TerminationProtected": False,
    "AdditionalMasterSecurityGroups": ["sg-XXXXXXX"],
    "AdditionalSlaveSecurityGroups": ["sg-XXXXXXX"],
    "Ec2SubnetId": "subnet-XXXXXXX",
    "Ec2KeyName": "dev",
    "EmrManagedMasterSecurityGroup": "sg-XXXXXXX",
    "ServiceAccessSecurityGroup": "sg-XXXXXXX",
    "EmrManagedSlaveSecurityGroup": "sg-XXXXXXX",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}]
}

#################################################################################
# EMR Tags should be handled here with map type variable `EMR_TAGS`             #
#################################################################################
EMR_TAGS = [
    {
        "Key": "Name",
        "Value": "Adhoc.EMR.Airflow",
    }
]

#################################################################################
# Step command which gets fed to the `EmrAddStepsOperator` operator             #
#################################################################################
STEPS = [
    {
        "Name": EMR_NAME + "-step01",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": SPARK_SUBMIT_ARGUMENTS + SPARK_JOB_ARGUMENTS
        }
    }
]

##################################################################################
# This is EMR Configuration usually known as `RunJobFlow`, one should not need   #
# to change them, until following situations                                     #
# 1. Changing the VPC, Subnets, Security Group, SSHKeys etc.                     #
# 2. Adding more spark configuration and classification, to read more about them #
#    https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#
# 3. Adding additional BootStrapActions                                          #
# 4. Adding more EMR JSON Config, read more about them                           #
#    https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html     #
#    Any addition to EMR JSON Config should be done in `EMR_INFRA_SETTING`       #
#    variable to make sure the readability of the code.                          #
##################################################################################
EMR_CONFIG = {
    "Name": EMR_NAME,
    "LogUri": EMR_CLUSTER_REQUIREMENTS["EMR_LOG_URI"],
    "ReleaseLabel": EMR_CLUSTER_REQUIREMENTS["EMR_VERSION"],
    "CustomAmiId": EMR_CLUSTER_REQUIREMENTS["CUSTOM_AMI"],
    "Configurations": [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ],
    "BootstrapActions": [],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": EMR_CLUSTER_REQUIREMENTS["MASTER_INSTANCE_TYPE"],
                "InstanceCount": EMR_CLUSTER_REQUIREMENTS["MASTER_INSTANCE_COUNT"],
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": EMR_CLUSTER_REQUIREMENTS["CORE_INSTANCE_TYPE"],
                "InstanceCount": EMR_CLUSTER_REQUIREMENTS["CORE_INSTANCE_COUNT"],
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": EMR_INFRA_SETTING["KeepJobFlowAliveWhenNoSteps"],
        "TerminationProtected": EMR_INFRA_SETTING["TerminationProtected"],
        "AdditionalMasterSecurityGroups": EMR_INFRA_SETTING["AdditionalMasterSecurityGroups"],
        "AdditionalSlaveSecurityGroups": EMR_INFRA_SETTING["AdditionalSlaveSecurityGroups"],
        "Ec2SubnetId": EMR_INFRA_SETTING["Ec2SubnetId"],
        "Ec2KeyName": EMR_INFRA_SETTING["Ec2KeyName"],
        "EmrManagedMasterSecurityGroup": EMR_INFRA_SETTING["EmrManagedMasterSecurityGroup"],
        "ServiceAccessSecurityGroup": EMR_INFRA_SETTING["ServiceAccessSecurityGroup"],
        "EmrManagedSlaveSecurityGroup": EMR_INFRA_SETTING["EmrManagedSlaveSecurityGroup"],
    },
    "Applications": EMR_INFRA_SETTING["Applications"],
    "JobFlowRole": EMR_INFRA_SETTING["JobFlowRole"],
    "ServiceRole": EMR_INFRA_SETTING["ServiceRole"],
    "Tags": EMR_TAGS
}

with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None
) as dag:
    create_emr = EmrCreateJobFlowOperator(
        task_id='create_emr',
        job_flow_overrides=EMR_CONFIG,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )

    add_step = EmrAddStepsOperator(
        task_id='add_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr', key='return_value') }}",
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        steps=STEPS
    )

    check_step = EmrStepSensor(
        task_id='check_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )

    remove_emr = EmrTerminateJobFlowOperator(
        task_id='remove_emr',
        job_flow_id="{{ task_instance.xcom_pull('create_emr', key='return_value') }}",
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )
##################################################################################
# Performs the Dag formation based on the logical dependency                     #
##################################################################################
create_emr >> add_step >> check_step >> remove_emr
