from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 14),
}

with DAG(
    dag_id='open_meteo_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,  # Execução manual
    catchup=False,
    description='Orquestra a pipeline de clima Open-Meteo',
    tags=['open-meteo', 'pipeline'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    extract_data = PapermillOperator(
        task_id='01_extract_data',
        input_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/extraction/01_extract_open_meteo.ipynb',
        output_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/executed/01_extract_open_meteo_{{ ds_nodash }}.ipynb',
    )

    bronze_layer = PapermillOperator(
        task_id='02_bronze_layer',
        input_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/bronze/02_validate_bronze_open_meteo.ipynb',
        output_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/executed/02_validate_bronze_open_meteo_{{ ds_nodash }}.ipynb',
    )

    silver_layer = PapermillOperator(
        task_id='03_silver_layer',
        input_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/silver/03_transform_silver_open_meteo.ipynb',
        output_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/executed/03_transform_silver_open_meteo_{{ ds_nodash }}.ipynb',
    )

    gold_layer = PapermillOperator(
        task_id='04_gold_layer',
        input_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/gold/04_prepare_gold_open_meteo.ipynb',
        output_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/executed/04_prepare_gold_open_meteo_{{ ds_nodash }}.ipynb',
    )

    generate_dashboard = PapermillOperator(
        task_id='05_generate_dashboard',
        input_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/dashboard/05_dashboard_open_meteo.ipynb',
        output_nb='/home/kenote_ubuntu/projetos/Airflow/notebooks/executed/05_dashboard_open_meteo_{{ ds_nodash }}.ipynb',
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Definindo a ordem de execução
    start_pipeline >> extract_data >> bronze_layer >> silver_layer >> gold_layer >> generate_dashboard >> end_pipeline

