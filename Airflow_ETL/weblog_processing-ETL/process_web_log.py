from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import tarfile
import pandas as pd

default_args = {
'owner': 't2ebve00',
'start_date': days_ago(0),
'email': ['data.eng@nomail.com'],
'email_on_failure': True,
'email_on_retry': True,
'retries': 1,
'retries_delay': timedelta(minutes = 5),
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Capstone assignment on Apache Airflow',
    schedule_interval = timedelta(days = 1),
)

# I am using PythonOperator for this task.
# Create a task to extract data
def extract_data():    
    file = '/opt/airflow/dags/capstone/accesslog.txt'
    with open(file) as log:
        file_df = pd.DataFrame([line.strip().split() for line in log.readlines()])
        file_df.to_csv('/opt/airflow/dags/capstone/accesslog.csv', index = False)

    log_df = pd.read_csv('/opt/airflow/dags/capstone/accesslog.csv')
    ip_addresses = log_df.iloc[:, [0]]
    new_column_names = ['ip_addresses']
    ip_addresses.columns = new_column_names
    ip_addresses.to_csv('/opt/airflow/dags/capstone/extracted_data.txt', index = False)

task1 =PythonOperator(
    task_id = "extract_data",
    python_callable = extract_data,
    dag=dag,
)

# Create a task to transform the data in the txt file
def transform_data():
    df = pd.read_table('/opt/airflow/dags/capstone/extracted_data.txt')
    filtered_ip = df[(df['ip_addresses'] == "198.46.149.143")]
    filtered_ip.to_csv('/opt/airflow/dags/capstone/transformed_data.txt', index = False)

task2 =PythonOperator(
    task_id = "transform_data",
    python_callable = transform_data,
    dag=dag,
)

# Create a task to load the data
def load_data():
    file = '/opt/airflow/dags/capstone/transformed_data.txt'
    new_file = "/opt/airflow/dags/capstone/weblog.tar"
    tar_file = tarfile.open(new_file, "w")
    tar_file.add(file)
    tar_file.close()

task3 =PythonOperator(
    task_id = "load_data",
    python_callable = load_data,
    dag=dag,
)


task1 >> task2 >> task3