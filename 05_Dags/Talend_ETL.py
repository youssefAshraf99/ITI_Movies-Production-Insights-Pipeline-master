# import Libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Dag Configuration
with DAG(
    dag_id='Talend_ETL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,10, 9),
    catchup=False
) as dag: 
      
    movie = BashOperator(
        task_id='MovieDimTalendJob',
        bash_command='/home/el-neshwy/virtualenvironment/airflow/talendETL/MovieDim_0.1/MovieDim/MovieDim_run.sh '
    )

    person = BashOperator(
        task_id='PersonDimTalendJob',
        bash_command='/home/el-neshwy/virtualenvironment/airflow/talendETL/PersonDim_0.1/PersonDim/PersonDim_run.sh '
    )

    production = BashOperator(
        task_id='ProductionDimTalendJob',
        bash_command='/home/el-neshwy/virtualenvironment/airflow/talendETL/ProductionDim_0.1/ProductionDim/ProductionDim_run.sh '
    )

    date = BashOperator(
        task_id='DateDimTalendJob',
        bash_command='/home/el-neshwy/virtualenvironment/airflow/talendETL/DateDim_0.1/DateDim/DateDim_run.sh '
    )

    fact = BashOperator(
        task_id='FactDimTalendJob',
        bash_command='/home/el-neshwy/virtualenvironment/airflow/talendETL/FactTable_0.1/FactTable/FactTable_run.sh '
    )

    [ movie, person, production, date] >> fact
