"""
Universal ETL DAG Factory

- Разделение на Extract, Transform, Load
- CSV-бэкапы с хранением 10 дней
- Системные колонки: _source_schema, _company_id, _loaded_at, _etl_job_id, _row_hash
- Конфиг: схемы и таблицы описываются по одному разу, DAG'и генерируются автоматически
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path
import logging
import pickle

DAGS_PATH = Path(__file__).parent
CONFIG_PATH = DAGS_PATH / 'config' / 'tables.yaml'
BACKUP_PATH = DAGS_PATH / 'backups'
TEMP_PATH = DAGS_PATH / 'temp'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def expand_config(raw_config: dict) -> dict:
    """
    Разворачивает компактный конфиг (schemas + tables) в плоский список table_config'ов.
    
    Поддерживает:
      - schemas_only: [finboon]  — таблица только для указанных схем
      - schedule на уровне таблицы переопределяет schedule схемы
      - target_table переопределяет имя целевой таблицы (по умолчанию = table)
    """
    schemas = raw_config.get('schemas', {})
    
    # Если schemas нет — значит старый формат, возвращаем как есть
    if not schemas:
        return raw_config
    
    flat_tables = []
    
    for table_def in raw_config.get('tables', []):
        # Какие схемы использовать
        allowed = table_def.get('schemas_only', list(schemas.keys()))
        
        for schema_name in allowed:
            if schema_name not in schemas:
                logger.warning(f"Схема '{schema_name}' не найдена в schemas, пропускаю")
                continue
            
            schema_conf = schemas[schema_name]
            
            flat_tables.append({
                'source_schema': schema_name,
                'source_table': table_def['table'],
                'target_table': table_def.get('target_table', table_def['table']),
                'primary_key': table_def['primary_key'],
                'schedule': table_def.get('schedule', schema_conf.get('schedule', '@daily')),
                'description': f"{table_def.get('description', table_def['table'])} ({schema_name})",
            })
    
    # Строим companies из schemas
    raw_config['companies'] = {
        name: conf['company_id'] for name, conf in schemas.items()
    }
    raw_config['tables'] = flat_tables
    
    return raw_config


def extract(table_config: dict, **context) -> dict:
    """Extract: Выгрузка данных из источника"""
    from etl.utils import load_config, generate_run_id
    from etl.connections import source_connection
    from etl.extractor import DataExtractor
    
    config = expand_config(load_config(CONFIG_PATH))
    settings = config.get('settings', {})
    
    source_schema = table_config['source_schema']
    source_table = table_config['source_table']
    
    # Job ID
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    job_id = generate_run_id(dag_id, run_id)
    
    logger.info(f"=== EXTRACT Start ===")
    logger.info(f"Source: {source_schema}.{source_table}")
    logger.info(f"Job ID: {job_id}")
    
    # Создаём temp директорию
    TEMP_PATH.mkdir(parents=True, exist_ok=True)
    
    with source_connection(config['source_db']) as source_conn:
        extractor = DataExtractor(source_conn, BACKUP_PATH)
        
        # Извлекаем данные
        columns, rows = extractor.extract(
            schema=source_schema,
            table=source_table,
            mode='full',
        )
        
        logger.info(f"Извлечено {len(rows)} строк")
        
        if not rows:
            return {
                'job_id': job_id,
                'rows_extracted': 0,
                'temp_file': None,
                'columns': [],
            }
        
        # CSV-бэкап
        csv_backup = None
        if settings.get('csv_backup_enabled', True):
            backup_name = f"{source_schema}__{source_table}"
            csv_path = extractor.save_csv_backup(
                rows=rows,
                columns=columns,
                table_name=backup_name,
                run_id=job_id,
            )
            csv_backup = str(csv_path)
            
            retention = settings.get('csv_retention_days', 10)
            extractor.cleanup_old_backups(backup_name, retention)
        
        # Сохраняем данные во временный файл (pickle для скорости)
        temp_file = TEMP_PATH / f"{job_id}_data.pkl"
        with open(temp_file, 'wb') as f:
            pickle.dump({'columns': columns, 'rows': rows}, f)
        
        logger.info(f"=== EXTRACT Complete ===")
        
        return {
            'job_id': job_id,
            'rows_extracted': len(rows),
            'temp_file': str(temp_file),
            'columns': columns,
            'csv_backup': csv_backup,
        }


def transform(table_config: dict, **context) -> dict:
    """Transform: Анализ и преобразование данных"""
    from etl.utils import load_config
    from etl.type_mapper import TypeMapper
    
    # Получаем результат extract через XCom
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    
    if not extract_result or not extract_result.get('temp_file'):
        logger.info("Нет данных для трансформации")
        return {'rows_transformed': 0, 'columns_meta': []}
    
    config = expand_config(load_config(CONFIG_PATH))
    
    logger.info(f"=== TRANSFORM Start ===")
    logger.info(f"Job ID: {extract_result['job_id']}")
    
    # Загружаем данные из временного файла
    temp_file = Path(extract_result['temp_file'])
    with open(temp_file, 'rb') as f:
        data = pickle.load(f)
    
    columns = data['columns']
    rows = data['rows']
    
    # Анализ типов
    type_mapper = TypeMapper(config['source_db']['type'])
    columns_meta = type_mapper.analyze_columns(columns, rows)
    
    # Преобразование данных
    transformed_rows = []
    for row in rows:
        converted = type_mapper.convert_row(row, columns_meta)
        transformed_rows.append(converted)
    
    # Сохраняем преобразованные данные
    transform_file = TEMP_PATH / f"{extract_result['job_id']}_transformed.pkl"
    with open(transform_file, 'wb') as f:
        pickle.dump({
            'columns_meta': columns_meta,
            'rows': transformed_rows,
            'original_rows': rows,  # для хеша
        }, f)
    
    logger.info(f"Преобразовано {len(transformed_rows)} строк")
    logger.info(f"=== TRANSFORM Complete ===")
    
    return {
        'job_id': extract_result['job_id'],
        'rows_transformed': len(transformed_rows),
        'columns_meta': columns_meta,
        'transform_file': str(transform_file),
    }


def load(table_config: dict, **context) -> dict:
    """Load: Загрузка данных в ClickHouse"""
    from etl.utils import load_config
    from etl.connections import clickhouse_connection
    from etl.type_mapper import TypeMapper
    from etl.loader import ClickHouseLoader
    
    # Получаем результаты предыдущих шагов
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    transform_result = ti.xcom_pull(task_ids='transform')
    
    if not transform_result or not transform_result.get('transform_file'):
        logger.info("Нет данных для загрузки")
        return {'rows_loaded': 0}
    
    config = expand_config(load_config(CONFIG_PATH))
    settings = config.get('settings', {})
    companies = config.get('companies', {})
    
    source_schema = table_config['source_schema']
    target_table = table_config['target_table']
    primary_key = table_config['primary_key']
    company_id = companies.get(source_schema, source_schema)
    job_id = extract_result['job_id']
    
    logger.info(f"=== LOAD Start ===")
    logger.info(f"Target: {target_table}")
    logger.info(f"Company: {company_id}")
    logger.info(f"Job ID: {job_id}")
    
    # Загружаем преобразованные данные
    transform_file = Path(transform_result['transform_file'])
    with open(transform_file, 'rb') as f:
        data = pickle.load(f)
    
    columns_meta = data['columns_meta']
    rows = data['rows']
    original_rows = data['original_rows']
    
    with clickhouse_connection(config.get('clickhouse', {})) as ch_conn:
        type_mapper = TypeMapper(config['source_db']['type'])
        loader = ClickHouseLoader(ch_conn, type_mapper)
        
        # Создаём таблицу
        loader.create_table(
            table=target_table,
            columns=columns_meta,
            primary_key=primary_key,
        )
        
        # Удаляем старые данные этой схемы
        loader.delete_schema_data(target_table, source_schema)
        
        # Загружаем
        batch_size = settings.get('batch_size', 50000)
        loaded = loader.load_data_transformed(
            table=target_table,
            columns=columns_meta,
            rows=rows,
            original_rows=original_rows,
            source_schema=source_schema,
            company_id=company_id,
            job_id=job_id,
            batch_size=batch_size,
        )
        
        # Оптимизация
        loader.optimize_table(target_table)
    
    # Очищаем временные файлы
    try:
        Path(extract_result['temp_file']).unlink(missing_ok=True)
        transform_file.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Не удалось удалить временные файлы: {e}")
    
    logger.info(f"=== LOAD Complete: {loaded} строк ===")
    
    return {
        'job_id': job_id,
        'rows_loaded': loaded,
        'target_table': target_table,
        'company_id': company_id,
    }


def create_dag(table_config: dict, default_args: dict) -> DAG:
    """Создаёт DAG для таблицы с E-T-L тасками"""
    source_schema = table_config['source_schema']
    source_table = table_config['source_table']
    target_table = table_config['target_table']
    
    dag_id = f"etl_{source_schema}__{source_table}"
    schedule = table_config.get('schedule', '@daily')
    description = table_config.get('description', f"ETL: {source_schema}.{source_table}")
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule=schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=['etl', 'clickhouse', f'schema:{source_schema}', f'table:{target_table}'],
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        extract_task = PythonOperator(
            task_id='extract',
            python_callable=extract,
            op_kwargs={'table_config': table_config},
        )
        
        transform_task = PythonOperator(
            task_id='transform',
            python_callable=transform,
            op_kwargs={'table_config': table_config},
        )
        
        load_task = PythonOperator(
            task_id='load',
            python_callable=load,
            op_kwargs={'table_config': table_config},
            retries=2,
            retry_delay=timedelta(minutes=5),
        )
        
        end = EmptyOperator(task_id='end')
        
        start >> extract_task >> transform_task >> load_task >> end
    
    return dag


# === Загрузка конфига и создание DAG'ов ===

try:
    from etl.utils import load_config
    config = expand_config(load_config(CONFIG_PATH))
except Exception as e:
    logger.error(f"Ошибка загрузки конфига: {e}")
    config = {'tables': []}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

for table_config in config.get('tables', []):
    dag = create_dag(table_config, default_args)
    globals()[dag.dag_id] = dag
    logger.info(f"DAG: {dag.dag_id}")