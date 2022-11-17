from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': "Caique-Evelyn",
    "depends_on_past": False,
    'start_date': datetime(2022, 11, 10)  # YYYY, MM, DD
}


@dag(
    default_args=default_args,
    schedule_interval="@once",
    description="Executa um job Spark no EMR",
    catchup=False,
    tags=['Spark', 'EMR', 'Business', 'Imdb']
)
def indicadores_businessImdb():

    inicio = DummyOperator(task_id='inicio')

    @task
    def criando_cluster_emr():
        cluster_id = client.run_job_flow(
            Name='Automated_EMR_Evelyn',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://airflow-puc/logs/emr-logs/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Task nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'TASK',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0091abc2305ce8b35'
            },
            },

            Applications=[{'Name': 'Spark'}],
        )
        return cluster_id["JobFlowId"]

    @task
    def aguardando_criacao_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True

    @task
    def enviando_dados_para_processamento(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Business',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--master', 'yarn',
                                     '--deploy-mode', 'cluster',
                                     's3://airflow-puc/files/business_imdb.py'
                                     ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]

    @task
    def aguardando_execucao_do_job(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )
    processoSucess = DummyOperator(task_id="processamento_concluido")

    @task
    def terminando_cluster_emr(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = DummyOperator(task_id="fim")

    # Orquestração

    cluster = criando_cluster_emr()
    inicio >> cluster

    esperacluster = aguardando_criacao_cluster(cluster)

    indicadores = enviando_dados_para_processamento(cluster)
    esperacluster >> indicadores

    wait_step = aguardando_execucao_do_job(cluster, indicadores)

    terminacluster = terminando_cluster_emr(cluster)
    wait_step >> processoSucess >> terminacluster >> fim


execucao = indicadores_businessImdb()
