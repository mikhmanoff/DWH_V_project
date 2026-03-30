"""Маппинг типов данных"""

from decimal import Decimal
from datetime import datetime, date
from typing import Any, List, Dict
import logging

logger = logging.getLogger(__name__)


class TypeMapper:
    def __init__(self, db_type: str):
        self.db_type = db_type
    
    def infer_from_value(self, value: Any) -> str:
        if value is None:
            return 'String'
        if isinstance(value, bool):
            return 'UInt8'
        elif isinstance(value, int):
            return 'Int64'
        elif isinstance(value, float):
            return 'Float64'
        elif isinstance(value, Decimal):
            return 'Float64'
        elif isinstance(value, datetime):
            return 'DateTime'
        elif isinstance(value, date):
            return 'Date'
        return 'String'
    
    def infer_from_sample(self, values: List[Any]) -> str:
        types_found = set()
        for value in values:
            if value is not None:
                types_found.add(self.infer_from_value(value))
        
        if not types_found:
            return 'String'
        if 'String' in types_found:
            return 'String'
        elif 'Float64' in types_found:
            return 'Float64'
        elif 'Int64' in types_found:
            return 'Int64'
        elif 'DateTime' in types_found:
            return 'DateTime'
        elif 'Date' in types_found:
            return 'Date'
        return list(types_found)[0]
    
    def analyze_columns(self, columns: List[str], rows: List[tuple], sample_size: int = 1000) -> List[Dict]:
        result = []
        sample = rows[:sample_size]
        
        for idx, col_name in enumerate(columns):
            values = [row[idx] for row in sample]
            ch_type = self.infer_from_sample(values)
            
            # Проверяем NULL во ВСЕХ данных, не только в sample
            has_nulls = any(row[idx] is None for row in rows)
            if has_nulls:
                ch_type = f'Nullable({ch_type})'
            
            result.append({'name': col_name, 'ch_type': ch_type, 'has_nulls': has_nulls})
        
        logger.info(f"Проанализировано {len(columns)} колонок")
        return result
    
    def convert_value(self, value: Any, ch_type: str) -> Any:
        if value is None:
            return None
        
        base_type = ch_type.replace('Nullable(', '').replace(')', '')
        
        try:
            if base_type == 'UInt8':
                return 1 if value else 0
            elif base_type == 'Int64':
                return int(value)
            elif base_type == 'Float64':
                return float(value)
            elif base_type == 'DateTime':
                if isinstance(value, date) and not isinstance(value, datetime):
                    return datetime.combine(value, datetime.min.time())
                return value
            elif base_type == 'Date':
                if isinstance(value, datetime):
                    return value.date()
                return value
            elif base_type == 'String':
                return str(value) if value is not None else ''
            return str(value) if value is not None else ''
        except Exception as e:
            logger.warning(f"Ошибка конвертации {value} в {ch_type}: {e}")
            return str(value) if value is not None else ''
    
    def convert_row(self, row: tuple, columns_meta: List[Dict]) -> tuple:
        return tuple(
            self.convert_value(value, columns_meta[idx]['ch_type'])
            for idx, value in enumerate(row)
        )
