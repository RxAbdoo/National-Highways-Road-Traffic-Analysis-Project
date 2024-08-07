
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

default_arguments={
 'owner':'abdo',
 'start_date':days_ago(0),
 'email':['abdelrhmanosama42@gmail.com'],
 'email_on_failure':True,
 'email_on_retry':True,
 'retries':1,
 'retry_delay':timedelta(minutes=5)

}

dag= DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_arguments,
    description='Apache Airflow Final Assignment'
    
)
#path for finalassignment folder
finalassignment='/home/project/airflow/dags/finalassignment'

#path for tolldata folder
tolldata=f'{finalassignment}/tolldata'
#path for final staging folder
staging=f'{finalassignment}/staging'

unzip_data=BashOperator(
task_id='unzip_data',
bash_command=f'tar -xzf {finalassignment}/tolldata.tgz -C {tolldata}',
dag=dag

)

def extractcsv():
    columns=['Rowid','Timestamp','Anonymized Vehicle number','Vehicle type']
    df_csv=pd.read_csv(f'{tolldata}/vehicle-data.csv',names=columns)
    extracted_df = df_csv[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    extracted_df.to_csv(f'{staging}/csv_data.csv',index=False)
    print('extracted csv succesfully')

extract_data_from_csv=PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extractcsv,
    dag=dag
)

def extracttsv():
    columns=['Number of axles','Tollplaza id','Tollplaza code']
    df_tsv=pd.read_csv(f'{tolldata}/tollplaza-data.tsv',sep='\t',names=columns)
    extracted_tsv=df_tsv[['Number of axles','Tollplaza id','Tollplaza code']]
    extracted_tsv.to_csv(f'{staging}/tsv_data.csv',index=False)
    print('extracted tsv succesfully')

extract_data_from_tsv=PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extracttsv,
    dag=dag
)

def extractwidth():
    columns=['Type of Payment code','Vehicle Code']
    df_ffw=pd.read_fwf(f'{tolldata}/payment-data.txt',colspecs=[(0, 4), (5, 10)],names=columns)
    extracted_ffw=df_ffw[['Type of Payment code','Vehicle Code']]
    extracted_ffw.to_csv(f'{staging}/fixed_width_data.csv',index=False)
    print('extracted ffw succesfully')

extract_data_from_fixed_width=PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extractwidth,
    dag=dag
)


def consolidate_data():
    csv_df=pd.read_csv(f'{staging}/csv_data.csv')
    tsv_df=pd.read_csv(f'{staging}/tsv_data.csv')
    ffw_df=pd.read_csv(f'{staging}/fixed_width_data.csv')
    df_concat=pd.concat([csv_df,tsv_df,ffw_df],axis=1)
    df_concat.to_csv(f'{staging}/extracted_data.csv',index=False)

consolidate_data= PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag
)

def transofrmdata():
    extracted_df=pd.read_csv(f'{staging}/extracted_data.csv')
    extracted_df['Vehicle type']=extracted_df['Vehicle type'].str.upper()
    trans_df=extracted_df
    trans_df.to_csv(f'{staging}/transformed_data.csv',index=False)

transform_data= PythonOperator(
    task_id='transform_data',
    python_callable=transofrmdata,
    dag=dag
)
unzip_data>>extract_data_from_csv>>extract_data_from_tsv>>extract_data_from_fixed_width>>consolidate_data>>transform_data