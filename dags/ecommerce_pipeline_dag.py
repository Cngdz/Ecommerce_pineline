from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

from scripts.load_raw_data import run as load_data_run

DBT_PROJECT_DIR = '/opt/airflow/dbt_project'

with DAG(
    dag_id='ecommerce_pipeline_v5',
    start_date=datetime(2025, 8, 12),
    schedule='@daily', # Chạy hàng ngày
    catchup=False,
    tags=['ecommerce'],
) as dag:
    load_raw_data_task = PythonOperator(
        task_id='load_raw_data_to_postgres',
        python_callable=load_data_run # Gọi hàm run đã import
    )

    # Dòng 27: Task 2 - Chạy dbt để biến đổi dữ liệu
    dbt_run_task = BashOperator(
        task_id='run_dbt_models',
        # Chạy lệnh 'dbt run' trong thư mục project dbt
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    # Dòng 33: Task 3 - Chạy dbt test để kiểm tra chất lượng dữ liệu
    dbt_test_task = BashOperator(
        task_id='test_dbt_models',
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )
    
    # Dòng 38: Thiết lập mối quan hệ phụ thuộc (dependency) giữa các task
    load_raw_data_task >> dbt_run_task >> dbt_test_task