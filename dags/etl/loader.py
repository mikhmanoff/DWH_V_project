"""Загрузка данных в ClickHouse"""

from typing import List, Dict, Any
from datetime import datetime
import hashlib
import logging

from etl.connections import ClickHouseConnection
from etl.type_mapper import TypeMapper

logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Загрузка данных в ClickHouse"""
    
    SYSTEM_COLUMNS = [
        {'name': '_source_schema', 'ch_type': 'String'},
        {'name': '_company_id', 'ch_type': 'String'},
        {'name': '_loaded_at', 'ch_type': 'DateTime'},
        {'name': '_etl_job_id', 'ch_type': 'String'},
        {'name': '_row_hash', 'ch_type': 'String'},
    ]
    
    def __init__(self, connection: ClickHouseConnection, type_mapper: TypeMapper):
        self.connection = connection
        self.type_mapper = type_mapper
    
    @staticmethod
    def compute_row_hash(row: tuple) -> str:
        """Вычисляет MD5 хеш строки"""
        row_str = '|'.join(str(v) if v is not None else '' for v in row)
        return hashlib.md5(row_str.encode('utf-8')).hexdigest()
    
    @staticmethod
    def safe_value(value: Any, ch_type: str) -> Any:
        """Безопасное преобразование значения"""
        if value is None:
            if 'Nullable' in ch_type:
                return None
            # Для не-Nullable возвращаем дефолт
            if 'String' in ch_type:
                return ''
            if 'Int' in ch_type:
                return 0
            if 'Float' in ch_type or 'Decimal' in ch_type:
                return 0.0
            return ''
        return value
    
    def create_table(self, table: str, columns: List[Dict], primary_key: str):
        """Создаёт таблицу в ClickHouse"""
        all_columns = columns + self.SYSTEM_COLUMNS
        
        cols_ddl = [f"`{col['name']}` {col['ch_type']}" for col in all_columns]
        
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {', '.join(cols_ddl)}
            )
            ENGINE = ReplacingMergeTree(_loaded_at)
            ORDER BY (`_source_schema`, `{primary_key}`)
            SETTINGS index_granularity = 8192
        """
        
        self.connection.execute(ddl)
        logger.info(f"Таблица {table} готова")
    
    def delete_schema_data(self, table: str, source_schema: str):
        """Удаляет данные конкретной схемы"""
        try:
            self.connection.execute(
                f"ALTER TABLE {table} DELETE WHERE _source_schema = %(schema)s",
                {'schema': source_schema}
            )
            logger.info(f"Удалены данные схемы {source_schema} из {table}")
        except Exception as e:
            logger.warning(f"Не удалось удалить данные: {e}")
    
    def load_data(
        self,
        table: str,
        columns: List[Dict],
        rows: List[tuple],
        source_schema: str,
        company_id: str,
        job_id: str,
        batch_size: int = 50000,
    ) -> int:
        """Загружает данные (с преобразованием)"""
        if not rows:
            return 0
        
        col_names = [f"`{c['name']}`" for c in columns]
        col_names.extend([
            '`_source_schema`', '`_company_id`', '`_loaded_at`', 
            '`_etl_job_id`', '`_row_hash`'
        ])
        
        loaded_at = datetime.now()
        total_loaded = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            prepared_batch = []
            for row in batch:
                converted = self.type_mapper.convert_row(row, columns)
                # Применяем safe_value к каждому значению
                safe_converted = tuple(
                    self.safe_value(v, columns[idx]['ch_type']) 
                    for idx, v in enumerate(converted)
                )
                row_hash = self.compute_row_hash(row)
                prepared_batch.append(
                    safe_converted + (source_schema, company_id, loaded_at, job_id, row_hash)
                )
            
            self.connection.execute(
                f"INSERT INTO {table} ({', '.join(col_names)}) VALUES",
                prepared_batch
            )
            total_loaded += len(batch)
            logger.info(f"Загружено {total_loaded}/{len(rows)}")
        
        return total_loaded
    
    def load_data_transformed(
        self,
        table: str,
        columns: List[Dict],
        rows: List[tuple],
        original_rows: List[tuple],
        source_schema: str,
        company_id: str,
        job_id: str,
        batch_size: int = 50000,
    ) -> int:
        """Загружает уже преобразованные данные"""
        if not rows:
            return 0
        
        col_names = [f"`{c['name']}`" for c in columns]
        col_names.extend([
            '`_source_schema`', '`_company_id`', '`_loaded_at`', 
            '`_etl_job_id`', '`_row_hash`'
        ])
        
        loaded_at = datetime.now()
        total_loaded = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            original_batch = original_rows[i:i + batch_size]
            
            prepared_batch = []
            for row, orig_row in zip(batch, original_batch):
                # Применяем safe_value к каждому значению
                safe_row = tuple(
                    self.safe_value(v, columns[idx]['ch_type']) 
                    for idx, v in enumerate(row)
                )
                row_hash = self.compute_row_hash(orig_row)
                prepared_batch.append(
                    safe_row + (source_schema, company_id, loaded_at, job_id, row_hash)
                )
            
            self.connection.execute(
                f"INSERT INTO {table} ({', '.join(col_names)}) VALUES",
                prepared_batch
            )
            total_loaded += len(batch)
            logger.info(f"Загружено {total_loaded}/{len(rows)}")
        
        return total_loaded
    
    def optimize_table(self, table: str):
        """Оптимизация таблицы"""
        try:
            self.connection.execute(f'OPTIMIZE TABLE {table} FINAL')
            logger.info(f"Таблица {table} оптимизирована")
        except Exception as e:
            logger.warning(f"Ошибка оптимизации: {e}")
