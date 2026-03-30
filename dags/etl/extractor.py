"""Извлечение данных"""

import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Any
import logging

from etl.connections import SourceConnection

logger = logging.getLogger(__name__)


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
    
    def save_csv_backup(
        self,
        rows: List[tuple],
        columns: List[str],
        table_name: str,
        run_id: str,
    ) -> Path:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.backup_path / f"{table_name}_{timestamp}_{run_id}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)
            for row in rows:
                csv_row = [str(v) if v is not None else '' for v in row]
                writer.writerow(csv_row)
        
        logger.info(f"CSV сохранён: {filename} ({len(rows)} строк)")
        return filename
    
    def cleanup_old_backups(self, table_name: str, retention_days: int):
        cutoff = datetime.now() - timedelta(days=retention_days)
        deleted = 0
        for file in self.backup_path.glob(f"{table_name}_*.csv"):
            if datetime.fromtimestamp(file.stat().st_mtime) < cutoff:
                file.unlink()
                deleted += 1
        if deleted:
            logger.info(f"Удалено {deleted} старых бэкапов")
