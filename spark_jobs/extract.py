"""
Extract Task: Загрузка CSV файлов в PostgreSQL (raw schema)
Поддерживает рекурсивный поиск CSV файлов в подпапках
"""
import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, to_timestamp, to_date, when, isnan, isnull, explode, array, lit, concat_ws, lpad
import psycopg2
from psycopg2.extras import execute_values

def get_postgres_connection():
    """Создать подключение к PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'energy_analytics'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )

def process_hourly_columns_format(df):
    """
    Обработка формата с почасовыми колонками: LCLid, day, hh_0, hh_1, ..., hh_47
    Преобразует колонки hh_* в строки с timestamp
    """
    # Определяем ID колонку (LCLid или id)
    id_col = "LCLid" if "LCLid" in df.columns else "id"
    
    # Получаем все колонки hh_*
    hh_columns = [c for c in df.columns if c.startswith("hh_")]
    hh_columns.sort(key=lambda x: int(x.split("_")[1]))  # Сортируем по номеру: hh_0, hh_1, ...
    
    print(f"    Найдено {len(hh_columns)} почасовых колонок")
    
    # Преобразуем day в дату
    df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))
    
    # Создаем структуры для каждой колонки hh_*: (hour_offset, energy_value)
    from pyspark.sql.functions import struct
    
    # Создаем массив структур (hour_offset, energy)
    structs = []
    for i, hh_col in enumerate(hh_columns):
        hour_offset = i // 2  # hh_0, hh_1 = час 0; hh_2, hh_3 = час 1; и т.д.
        minute_offset = (i % 2) * 30  # 0 или 30 минут
        structs.append(struct(
            lit(hour_offset).alias("hour"),
            lit(minute_offset).alias("minute"),
            col(hh_col).cast("double").alias("energy_kwh")
        ))
    
    # Создаем массив и распаковываем его
    df = df.withColumn("hourly_data", array(*structs))
    
    # Распаковываем массив в строки
    df = df.select(
        col(id_col).alias("LCLid"),
        col("day"),
        explode(col("hourly_data")).alias("hourly")
    )
    
    # Извлекаем данные из структуры и создаем timestamp
    df = df.withColumn("hour", col("hourly.hour"))
    df = df.withColumn("minute", col("hourly.minute"))
    df = df.withColumn("energy_kwh", col("hourly.energy_kwh"))
    
    # Создаем timestamp из day + hour + minute
    df = df.withColumn(
        "timestamp",
        to_timestamp(
            concat_ws(" ", 
                col("day").cast("string"),
                concat_ws(":",
                    lpad(col("hour").cast("string"), 2, "0"),
                    lpad(col("minute").cast("string"), 2, "0"),
                    lit("00")
                )
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    
    # Удаляем временные колонки
    df = df.select("LCLid", "day", "energy_kwh", "timestamp")
    
    return df

def process_kaggle_format(df):
    """
    Обработка формата Kaggle: id, tstp, energy(kWh/hh)
    """
    # Определяем ID колонку
    id_col = "LCLid" if "LCLid" in df.columns else "id"
    
    # Находим колонку с энергией
    energy_col = None
    for col_name in df.columns:
        if "energy" in col_name.lower() and "kwh" in col_name.lower():
            energy_col = col_name
            break
    
    if energy_col is None:
        raise ValueError("Не найдена колонка с энергией в формате Kaggle")
    
    # Преобразование tstp в timestamp и day
    df = df.withColumn("timestamp", to_timestamp(col("tstp"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("day", to_date(col("timestamp")))
    df = df.withColumn("energy_kwh", col(energy_col).cast("double"))
    df = df.withColumn("LCLid", col(id_col).cast("string"))
    
    # Выбираем нужные колонки
    df = df.select("LCLid", "day", "energy_kwh", "timestamp")
    
    return df

def process_aggregated_format(df):
    """
    Обработка агрегированного формата: id, day, energy_median, energy_mean, energy_max, etc.
    Используем energy_mean или energy_sum как основное значение
    """
    # Определяем ID колонку
    id_col = "LCLid" if "LCLid" in df.columns else "id"
    
    # Выбираем колонку с энергией (приоритет: mean > sum > median)
    energy_col = None
    if "energy_mean" in df.columns:
        energy_col = "energy_mean"
    elif "energy_sum" in df.columns:
        energy_col = "energy_sum"
    elif "energy_median" in df.columns:
        energy_col = "energy_median"
    else:
        raise ValueError("Не найдена подходящая колонка с энергией (energy_mean, energy_sum, energy_median)")
    
    # Преобразуем day
    df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))
    df = df.withColumn("energy_kwh", col(energy_col).cast("double"))
    df = df.withColumn("LCLid", col(id_col).cast("string"))
    
    # Для агрегированных данных создаем timestamp как начало дня (00:00:00)
    df = df.withColumn(
        "timestamp",
        to_timestamp(concat_ws(" ", col("day").cast("string"), lit("00:00:00")), "yyyy-MM-dd HH:mm:ss")
    )
    
    df = df.select("LCLid", "day", "energy_kwh", "timestamp")
    
    return df

def process_standard_format(df):
    """
    Обработка стандартного формата: LCLid, day, energy_kwh, timestamp
    """
    # Определяем ID колонку
    id_col = "LCLid" if "LCLid" in df.columns else "id"
    
    if "day" in df.columns:
        df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    if "energy_kwh" in df.columns:
        df = df.withColumn("energy_kwh", col("energy_kwh").cast("double"))
    
    df = df.withColumn("LCLid", col(id_col).cast("string"))
    df = df.select("LCLid", "day", "energy_kwh", "timestamp")
    
    return df

def validate_csv_structure(df):
    """Валидация структуры CSV файла"""
    required_columns = ['LCLid', 'day', 'energy_kwh', 'timestamp']
    missing_columns = [c for c in required_columns if c not in df.columns]
    
    if missing_columns:
        available_cols = ", ".join(df.columns)
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}. Доступные колонки: {available_cols}")
    
    return True

def extract_data(spark, data_path):
    """
    Извлечение данных из CSV файлов (поддерживает рекурсивный поиск в подпапках)
    
    Args:
        spark: SparkSession
        data_path: путь к директории с CSV файлами (может содержать подпапки)
    
    Returns:
        DataFrame с загруженными данными
    """
    print(f"Загрузка данных из: {data_path}")
    
    # Рекурсивный поиск всех CSV файлов в подпапках
    csv_files = glob.glob(f"{data_path}/**/*.csv", recursive=True)
    
    if not csv_files:
        raise FileNotFoundError(f"Не найдены CSV файлы в {data_path} (включая подпапки)")
    
    print(f"Найдено CSV файлов: {len(csv_files)}")
    print(f"Примеры найденных файлов:")
    for f in csv_files[:3]:
        print(f"  - {f}")
    
    # Обрабатываем файлы по группам (по папкам) для правильного определения формата
    all_dataframes = []
    
    # Группируем файлы по папкам (предпоследний уровень в пути)
    from collections import defaultdict
    files_by_folder = defaultdict(list)
    for f in csv_files:
        # Извлекаем название папки (предпоследний элемент пути)
        parts = f.split('/')
        if len(parts) >= 2:
            folder_name = parts[-2]  # название папки, в которой находится файл
            files_by_folder[folder_name].append(f)
    
    print(f"\nНайдено папок с CSV: {len(files_by_folder)}")
    
    # Обрабатываем каждую папку отдельно
    for folder_name, folder_files in files_by_folder.items():
        print(f"\nОбработка папки: {folder_name} ({len(folder_files)} файлов)")
        
        # Загружаем CSV файлы из этой папки
        df_folder = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(folder_files)
        
        print(f"  Колонки: {df_folder.columns}")
        
        # Определяем формат по колонкам
        if "hh_0" in df_folder.columns or "hh_1" in df_folder.columns:
            # Формат 3: LCLid, day, hh_0, hh_1, ..., hh_47 (почасовые данные в колонках)
            print(f"  Формат 3: Почасовые данные в колонках (hh_0-hh_47)")
            df_folder = process_hourly_columns_format(df_folder)
            
        elif "tstp" in df_folder.columns and any("energy" in c.lower() and "kwh" in c.lower() for c in df_folder.columns):
            # Формат 2: id, tstp, energy(kWh/hh)
            print(f"  Формат 2: Kaggle формат (tstp, energy(kWh/hh))")
            df_folder = process_kaggle_format(df_folder)
            
        elif "energy_mean" in df_folder.columns or "energy_sum" in df_folder.columns or "energy_median" in df_folder.columns:
            # Формат 1: id, day, energy_median, energy_mean, energy_max, etc.
            print(f"  Формат 1: Агрегированные данные (energy_mean, energy_sum, etc.)")
            df_folder = process_aggregated_format(df_folder)
            
        else:
            # Пытаемся стандартный формат
            print(f"  Пытаемся стандартный формат")
            df_folder = process_standard_format(df_folder)
        
        all_dataframes.append(df_folder)
    
    # Объединяем все DataFrame
    if len(all_dataframes) > 1:
        print(f"\nОбъединение {len(all_dataframes)} датасетов...")
        df = all_dataframes[0]
        for df_part in all_dataframes[1:]:
            df = df.unionByName(df_part, allowMissingColumns=True)
    else:
        df = all_dataframes[0]
    
    # Валидация структуры
    validate_csv_structure(df)
    
    # Очистка данных: удаление строк с null значениями в ключевых полях
    df_cleaned = df.filter(
        col("LCLid").isNotNull() &
        col("day").isNotNull() &
        col("energy_kwh").isNotNull() &
        col("timestamp").isNotNull()
    )
    
    # Удаление дубликатов (используем LCLid, timestamp для уникальности, так как timestamp более точный)
    df_cleaned = df_cleaned.dropDuplicates(["LCLid", "timestamp"])
    
    # Не вызываем count() здесь, так как это требует материализации всего DataFrame в память
    # Вместо этого просто сообщаем, что данные готовы
    print("Данные обработаны и готовы к загрузке в PostgreSQL")
    
    return df_cleaned

def load_to_postgres(df, table_name="raw.energy_consumption", batch_size=5000):
    """
    Загрузка данных в PostgreSQL батчами для экономии памяти
    Использует toLocalIterator для обработки данных по частям
    
    Args:
        df: Spark DataFrame
        table_name: имя таблицы для загрузки
        batch_size: размер батча для загрузки
    """
    print(f"Загрузка данных в таблицу: {table_name} (батчами по {batch_size} записей)")
    
    selected_df = df.select("LCLid", "day", "energy_kwh", "timestamp")
    
    # Подключение к PostgreSQL (одно подключение на весь процесс)
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        total_loaded = 0
        batch_data = []
        
        # Используем toLocalIterator для обработки данных по частям
        print("Начало загрузки данных...")
        for row in selected_df.toLocalIterator():
            batch_data.append((
                row.LCLid,
                row.day,
                float(row.energy_kwh) if row.energy_kwh is not None else None,
                row.timestamp
            ))
            
            # Когда батч заполнен, загружаем в БД
            if len(batch_data) >= batch_size:
                insert_query = f"""
                    INSERT INTO {table_name} (lclid, day, energy_kwh, timestamp)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """
                execute_values(cursor, insert_query, batch_data, page_size=1000)
                conn.commit()
                total_loaded += len(batch_data)
                print(f"  Загружено: {total_loaded} записей...")
                batch_data = []
        
        # Загружаем оставшиеся данные
        if batch_data:
            insert_query = f"""
                INSERT INTO {table_name} (lclid, day, energy_kwh, timestamp)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cursor, insert_query, batch_data, page_size=1000)
            conn.commit()
            total_loaded += len(batch_data)
        
        print(f"Успешно загружено {total_loaded} записей в {table_name}")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main(data_path=None):
    """Главная функция Extract задачи"""
    import os
    
    # Путь к данным (может быть передан как аргумент функции или через sys.argv)
    if data_path is None:
        if len(sys.argv) > 1:
            data_path = sys.argv[1]
        else:
            # Пробуем стандартные пути для Airflow
            if os.path.exists("/opt/airflow/data/raw"):
                data_path = "/opt/airflow/data/raw"
            elif os.path.exists("/opt/spark/data/raw"):
                data_path = "/opt/spark/data/raw"
            else:
                data_path = "/opt/airflow/data/raw"  # По умолчанию
    
    # Проверка существования директории
    if not os.path.exists(data_path):
        print(f"Ошибка: директория {data_path} не существует")
        # Попробуем найти данные в других местах
        alternative_paths = [
            "/opt/airflow/data/raw",
            "/opt/spark/data/raw",
            "./data/raw"
        ]
        for alt_path in alternative_paths:
            if os.path.exists(alt_path):
                print(f"Найдена альтернативная директория: {alt_path}")
                data_path = alt_path
                break
        else:
            raise FileNotFoundError(f"Директория с данными не найдена. Проверенные пути: {alternative_paths}")
    
    # Создание SparkSession (локальный режим) с оптимизированными настройками памяти
    spark = SparkSession.builder \
        .appName("EnergyAnalytics-Extract") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()
    
    try:
        # Извлечение данных
        df = extract_data(spark, data_path)
        
        # Загрузка в PostgreSQL
        load_to_postgres(df)
        
        print("Extract задача выполнена успешно!")
        
    except Exception as e:
        print(f"Ошибка в Extract задаче: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

