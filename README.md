## Collection of Airflow Dags

| **Airflow Dag** | **Components** | **What it does**|
| --- | --- | --- |
| [on-demand-spark-on-emr.py](./on-demand-spark-on-emr.py) | `Spark, Emr` | Spins `EMR` on the fly and runs the `spark` code |
| [on-demand-remote-exec-with-ec2.py](./on-demand-remote-exec-with-ec2.py) | `EC2, Remote Execution`| Spins `EC2` on the fly and can be used to execute remote code |


[on-demand-remote-exec-with-ec2.py](./on-demand-remote-exec-with-ec2.py) uses the custom plugin [kodelint/airflow-ec2-plugin-extended](https://github.com/kodelint/airflow-ec2-plugin-extended). **Airflow** `1.10.12` uses [apache-airflow-backport-providers-amazon](https://pypi.org/project/apache-airflow-backport-providers-amazon/) does have support for [ec2](https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/operators/ec2.py) but only limited to start using [EC2StartInstanceOperator](https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/operators/ec2.py#L29) and stop using [EC2StopInstanceOperator](https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/operators/ec2.py#L75), given the `instance_id` is known. It is missing create and terminate functionality.