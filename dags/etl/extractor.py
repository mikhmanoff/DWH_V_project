"""Извлечение данных"""

from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Any
import logging

import pyarrow as pa
import pyarrow.parquet as pq

from etl.connections import SourceConnection

logger = logging.getLogger(__name__)


# Маппинг Python-типов → PyArrow-типов
def _infer_arrow_type(value: Any) -> pa.DataType:
    """Определяет PyArrow тип по значению Python"""
    from decimal import Decimal
    from datetime import datetime, date

    if isinstance(value, bool):
        return pa.bool_()
    elif isinstance(value, int):
        return pa.int64()
    elif isinstance(value, float):
        return pa.float64()
    elif isinstance(value, Decimal):
        return pa.float64()
    elif isinstance(value, datetime):
        return pa.timestamp('us')
    elif isinstance(value, date):
        return pa.date32()
    elif isinstance(value, bytes):
        return pa.binary()
    return pa.string()


def _infer_schema(columns: List[str], rows: List[tuple], sample_size: int = 500) -> pa.Schema:
    """Строит PyArrow-схему по sample данных"""
    sample = rows[:sample_size]
    fields = []

    for idx, col_name in enumerate(columns):
        # Находим первое не-None значение
        arrow_type = pa.string()  # дефолт
        for row in sample:
            if row[idx] is not None:
                arrow_type = _infer_arrow_type(row[idx])
                break
        fields.append(pa.field(col_name, arrow_type, nullable=True))

    return pa.schema(fields)


def rows_to_arrow_table(columns: List[str], rows: List[tuple]) -> pa.Table:
    """Конвертирует список строк в PyArrow Table"""
    if not rows:
        return pa.table({col: pa.array([], type=pa.string()) for col in columns})

    schema = _infer_schema(columns, rows)

    # Собираем колоночные массивы
    col_arrays = {}
    for idx, field in enumerate(schema):
        values = [row[idx] for row in rows]
        try:
            col_arrays[field.name] = pa.array(values, type=field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Fallback: конвертируем в строки
            str_values = [str(v) if v is not None else None for v in values]
            col_arrays[field.name] = pa.array(str_values, type=pa.string())

    return pa.table(col_arrays)


class DataExtractor:
    def __init__(self, connection: SourceConnection, backup_path: Path):
        self.connection = connection
        self.backup_path = backup_path
        self.backup_path.mkdir(parents=True, exist_ok=True)

    def extract(
        self,
        schema: str,
        table: str,
        mode: str = 'full',
        incremental_key: Optional[str] = None,
        last_value: Optional[Any] = None,
    ) -> Tuple[List[str], List[tuple]]:
        with self.connection.cursor() as cursor:
            query = f"SELECT * FROM {schema}.{table}"
            logger.info(f"Выгрузка: {schema}.{table}")
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            logger.info(f"Извлечено {len(rows)} строк, {len(columns)} колонок")
            return columns, rows

    def save_parquet_backup(
        self,
        rows: List[tuple],
        columns: List[str],
        table_name: str,
        run_id: str,
    ) -> Path:
        """Сохраняет бэкап в формате Parquet (zstd)"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.backup_path / f"{table_name}_{timestamp}_{run_id}.parquet"

        arrow_table = rows_to_arrow_table(columns, rows)
        pq.write_table(arrow_table, filename, compression='zstd')

        size_mb = filename.stat().st_size / (1024 * 1024)
        logger.info(f"Parquet сохранён: {filename} ({len(rows)} строк, {size_mb:.2f} MB)")
        return filename

    def save_extract_parquet(
        self,
        rows: List[tuple],
        columns: List[str],
        output_path: Path,
    ) -> Path:
        """Сохраняет извлечённые данные в parquet для передачи между шагами ETL"""
        arrow_table = rows_to_arrow_table(columns, rows)
        pq.write_table(arrow_table, output_path, compression='snappy')
        logger.info(f"Extract parquet: {output_path} ({len(rows)} строк)")
        return output_path

    def cleanup_old_backups(self, table_name: str, retention_days: int):
        cutoff = datetime.now() - timedelta(days=retention_days)
        deleted = 0
        # Чистим и parquet, и csv (на случай миграции)
        for pattern in (f"{table_name}_*.parquet", f"{table_name}_*.csv"):
            for file in self.backup_path.glob(pattern):
                if datetime.fromtimestamp(file.stat().st_mtime) < cutoff:
                    file.unlink()
                    deleted += 1
        if deleted:
            logger.info(f"Удалено {deleted} старых бэкапов")