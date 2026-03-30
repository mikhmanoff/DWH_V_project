"""Утилиты ETL"""

import yaml
from pathlib import Path
from typing import Dict, Any
import hashlib
import logging

logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> Dict[str, Any]:
    """Загружает конфигурацию из YAML"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"Конфигурация загружена: {config_path}")
    return config


def generate_run_id(dag_id: str, run_id: str) -> str:
    """Генерирует уникальный ID запуска"""
    short_hash = hashlib.md5(run_id.encode()).hexdigest()[:8]
    return f"{dag_id}_{short_hash}"
