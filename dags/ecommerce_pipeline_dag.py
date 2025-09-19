# ecommerce_pipeline_dag.py
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime
import os
import sys
# Thêm đường dẫn scripts vào PYTHONPATH
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/scripts')
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'

def upload_crawled_data(ti):
    from scripts.minio_client import upload_file_to_minio
    local_file_path = ti.xcom_pull(task_ids='crawl_data')

    if local_file_path:
        # Tạo tên file trên MinIO
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"raw_data/tiki_products_{timestamp}.json"        
        success = upload_file_to_minio("bronze", object_name, local_file_path)
        
        # Nếu upload thành công, xóa file cục bộ
        if success:
            os.remove(local_file_path)
            ti.xcom_push(key='bronze_path', value=f"s3a://bronze/{object_name}")
    else:
        print("No file path received from the previous task.")

def create_bucket(bucket_name):
    from scripts.silver_transform import ensure_bucket_exists
    ensure_bucket_exists(bucket_name)

with DAG(
    dag_id='ecommerce_pipeline_v6',
    start_date=datetime(2025, 8, 12),
    schedule='@daily', # Chạy hàng ngày
    catchup=False,
    tags=['ecommerce'],
) as dag:
    
    crawl_data_task = BashOperator(
        task_id='crawl_data',
        bash_command='cd /opt/airflow && scrapy runspider scripts/scraper.py -o data/tiki_products.json >/dev/null 2>&1 && echo "/opt/airflow/data/tiki_products.json"',
        do_xcom_push=True
    )

    load_data_bronze_task = PythonOperator(
        task_id='load_data_to_bronze',
        python_callable=upload_crawled_data
    )
    
    silver_path = f"s3a://silver/{{{{ ds }}}}"
    transform_to_silver_task = SparkSubmitOperator(
        task_id='transform_to_silver',
        application='/opt/airflow/scripts/silver_transform.py',
        name='silver_transformation',
        conn_id='spark_default',
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio_admin",
            "spark.hadoop.fs.s3a.secret.key": "password123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.timeout": 60000,
            "spark.hadoop.fs.s3a.attempts.timeout": 60000,
            "spark.hadoop.fs.s3a.connection.establishment.timeout": 5000,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.network.timeout": 600000,
            "spark.executor.heartbeatInterval": 120000,
            "spark.storage.blockManagerSlaveTimeoutMs": 600000,
            "spark.sql.broadcastTimeout": 600000
        },
        application_args=[
            "{{ ti.xcom_pull(task_ids='load_data_to_bronze', key='bronze_path') }}"
        ],
        env_vars={
            'PYSPARK_PYTHON': 'python',
            'PYSPARK_DRIVER_PYTHON': 'python'
        },
        verbose=True    
    )

    run_dbt_task = BashOperator(
        task_id='run_dbt',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .'
    )

    crawl_data_task >> load_data_bronze_task >> transform_to_silver_task >> run_dbt_task