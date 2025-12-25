"""
Airflow DAG для ETL пайплайна энергетической аналитики
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# Параметры по умолчанию
default_args = {
    'owner': 'energy_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'energy_analytics_etl',
    default_args=default_args,
    description='ETL пайплайн для анализа потребления электроэнергии',
    schedule_interval=timedelta(days=1),  # Запуск раз в день
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['energy', 'etl', 'spark', 'postgres'],
)

# Пути к данным и скриптам
DATA_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"
SPARK_JOBS_PATH = "/opt/airflow/spark_jobs"
SPARK_MASTER = "spark://spark-master:7077"

# Переменные окружения для Spark
spark_env = {
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB', 'energy_analytics'),
    'POSTGRES_USER': os.getenv('POSTGRES_USER', 'postgres'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'postgres'),
}

def check_data_availability():
    """Проверка наличия данных (рекурсивный поиск в подпапках)"""
    import glob
    import os
    # Рекурсивный поиск CSV файлов в подпапках
    csv_files = glob.glob(f"{DATA_PATH}/**/*.csv", recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"Не найдены CSV файлы в {DATA_PATH} (рекурсивный поиск)")
    print(f"Найдено CSV файлов: {len(csv_files)} (включая подпапки)")
    # Показываем первые несколько найденных файлов для диагностики
    print(f"Примеры найденных файлов:")
    for f in csv_files[:5]:
        print(f"  - {f}")
    return csv_files

# Task 1: Extract - Извлечение данных
def run_extract_task():
    """Запуск Extract задачи через PySpark"""
    import sys
    import os
    
    # Устанавливаем переменные окружения
    for key, value in spark_env.items():
        os.environ[key] = value
    
    # Добавляем путь к скриптам
    sys.path.insert(0, SPARK_JOBS_PATH)
    
    # Импортируем и запускаем extract с правильным путем
    from extract import main
    # Передаем путь к данным напрямую в функцию
    main(data_path=DATA_PATH)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=run_extract_task,
    dag=dag,
)

# Task 2: Transform - Трансформация данных
def run_transform_task():
    """Запуск Transform задачи через PySpark"""
    import sys
    import os
    
    # Устанавливаем переменные окружения
    for key, value in spark_env.items():
        os.environ[key] = value
    
    # Добавляем путь к скриптам
    sys.path.insert(0, SPARK_JOBS_PATH)
    
    # Импортируем и запускаем transform
    from transform import main
    main()

transform_task = PythonOperator(
    task_id='transform',
    python_callable=run_transform_task,
    dag=dag,
)

# Task 3: Load - Загрузка данных в DWH
def run_load_task():
    """Запуск Load задачи через PySpark"""
    import sys
    import os
    
    # Устанавливаем переменные окружения
    for key, value in spark_env.items():
        os.environ[key] = value
    
    # Добавляем путь к скриптам
    sys.path.insert(0, SPARK_JOBS_PATH)
    
    # Импортируем и запускаем load с правильным путем
    from load import main
    main(processed_path=PROCESSED_PATH)

load_task = PythonOperator(
    task_id='load',
    python_callable=run_load_task,
    dag=dag,
)

# Проверка наличия данных (опционально)
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

# Определение зависимостей между задачами
check_data_task >> extract_task >> transform_task >> load_task

