from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def load_files():
    df = pd.read_csv(r'./data/Books_df.csv')
    df.to_pickle(r'./data/Books_df.pkl')

def transform_data():
    df = pd.read_pickle(r'./data/Books_df.pkl')
    df = df.drop_duplicates(subset=['Title', 'Author'])
    df['Price'] = df['Price'].apply(lambda x: str(x)[1:])
    df['Price'] = df['Price'].apply(lambda x: round(float(x.replace(",", "")) / 20.61, 2))
    df = df[['Title', 'Author', 'Main Genre', 'Sub Genre', 'Type', 'Rating', 'No. of People rated', 'Price']]
    df.to_pickle(r'./data/Books_df.pkl')

def save_to_excel():
    df = pd.read_pickle(r'./data/Books_df.pkl')
    df.to_excel(r'./dashboard/Books_df.xlsx', index=False)
    os.remove(r"./data/Books_df.pkl")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'books_dashboard_ETL',
    default_args=default_args,
    description='Amazon Books Dashboard ETL',
    schedule_interval=timedelta(days=1),
) as dag:

    load_task = PythonOperator(
        task_id='load_excel_file',
        python_callable=load_files,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    save_task = PythonOperator(
        task_id='save_to_excel',
        python_callable=save_to_excel,
    )

    load_task >> transform_task >> save_task