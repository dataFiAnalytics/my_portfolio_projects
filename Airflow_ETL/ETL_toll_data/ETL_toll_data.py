from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import tarfile
import pandas as pd

default_args = {
'owner': 'Gabi',
'start_date': days_ago(0),
'email': ['data.mestari@yahoo.com'],
'email_on_failure': True,
'email_on_retry': True,
'retries': 1,
'retries_delay': timedelta(minutes = 5),
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval = timedelta(days = 1),
)

# I am using PythonOperator to complete this task
def unzip_data():
    tf = tarfile.open('/opt/airflow/dags/finalassignment/tolldata.tgz')
    tf.extractall('/opt/airflow/dags/finalassignment')
    tf.close()
  
task1 =PythonOperator(
    task_id = "unzip_data",
    python_callable = unzip_data,
    dag=dag,
)

# I am using PythonOperator to complete this task
def extract_data_from_csv():
    csv_data = '/opt/airflow/dags/finalassignment/vehicle-data.csv'
    vehicle_df = pd.read_csv(csv_data)
    new_column_names = ['Rowid', 'Timestamp', 'Anonymized_vehicle_number', 'Vehicle_type', 'Number_of_axles', 'Vehicle_type_ID']
    vehicle_df.columns = new_column_names
    new_vehicle_df = vehicle_df.drop(["Number_of_axles", "Vehicle_type_ID"], axis = 1)
    new_vehicle_df.to_csv('/opt/airflow/dags/finalassignment/csv_data.csv', index = False)

task2 =PythonOperator(
    task_id = "extract_data_from_csv",
    python_callable = extract_data_from_csv,
    dag=dag,
)

# I am using PythonOperator to complete this task
def extract_data_from_tsv():
    tsv_data = '/opt/airflow/dags/finalassignment/tollplaza-data.tsv'
    tollplaza_df = pd.read_table(tsv_data, header = None)
    new_column_names = ['Rowid', 'Timestamp', 'Anonymized_vehicle_number', 'Vehicle_type', 'Number_of_axles', 'Tollplaza_id', 'Tollplaza_code']
    tollplaza_df.columns = new_column_names
    new_tollplaza_df = tollplaza_df[["Number_of_axles", "Tollplaza_id", "Tollplaza_code"]]
    new_tollplaza_df.to_csv('/opt/airflow/dags/finalassignment/tsv_data.csv', index = False)

task3 =PythonOperator(
    task_id = "extract_data_from_tsv",
    python_callable = extract_data_from_tsv,
    dag=dag,
)

# I am using PythonOperator to complete this task
def extract_data_from_fixed_width():
    with open('/opt/airflow/dags/finalassignment/payment-data.txt') as p_data:
        paymentData_df = pd.DataFrame([line.strip().split() for line in p_data.readlines()])
        paymentData_df.to_csv('/opt/airflow/dags/finalassignment/payment-data.csv', index = False)

    pData_df = pd.read_csv('/opt/airflow/dags/finalassignment/payment-data.csv')
    new_pData_df = pData_df.iloc[:, [9, 10]]
    new_column_names = ['Type_of_Payment_code', 'Vehicle_code']
    new_pData_df.columns = new_column_names
    new_pData_df.to_csv('/opt/airflow/dags/finalassignment/fixed_width_data.csv', index = False)

task4 =PythonOperator(
    task_id = "extract_data_from_fixed_width",
    python_callable = extract_data_from_fixed_width,
    dag=dag,
)

# I am using PythonOperator to complete this task
def consolidate_data():
    csv_data = pd.read_csv('/opt/airflow/dags/finalassignment/csv_data.csv')
    tsv_data = pd.read_csv('/opt/airflow/dags/finalassignment/tsv_data.csv')
    pay_data = pd.read_csv('/opt/airflow/dags/finalassignment/fixed_width_data.csv')
    merged_data = pd.concat([csv_data, tsv_data, pay_data], axis = 1)
    merged_data.to_csv('/opt/airflow/dags/finalassignment/extracted_data.csv', index = False)

task5 =PythonOperator(
    task_id = "consolidate_data",
    python_callable = consolidate_data,
    dag=dag,
)

# I am using PythonOperator to complete this task
def transform_data():
    data = '/opt/airflow/dags/finalassignment/extracted_data.csv'
    data_df = pd.read_csv(data)
    data_df['Vehicle_type'] = data_df['Vehicle_type'].str.upper()
    data_df.to_csv('/opt/airflow/dags/finalassignment/staging/transformed_data.csv', index = False)

task6 =PythonOperator(
    task_id = "transform_data",
    python_callable = transform_data,
    dag=dag,
)


task1 >> task2 >> task3 >> task4 >> task5 >> task6

