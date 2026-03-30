"""Управление подключениями к БД"""

from contextlib import contextmanager
from typing import Any, Generator
import logging

logger = logging.getLogger(__name__)


class SourceConnection:
    def __init__(self, config: dict):
        self.config = config
        self.db_type = config['type']
        self._connection = None
    
    def connect(self):
        if self.db_type == 'postgresql':
            import psycopg2
            self._connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=30,
            )
        elif self.db_type in ('mysql', 'mariadb'):
            import pymysql
            self._connection = pymysql.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=30,
                charset='utf8mb4',
            )
        elif self.db_type == 'mssql':
            import pymssql
            self._connection = pymssql.connect(
                server=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                timeout=30,
            )
        else:
            raise ValueError(f"Неподдерживаемый тип БД: {self.db_type}")
        
        logger.info(f"Подключено к {self.db_type}://{self.config['host']}/{self.config['database']}")
        return self._connection
    
    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
    
    @contextmanager
    def cursor(self) -> Generator[Any, None, None]:
        cur = self._connection.cursor()
        try:
            yield cur
        finally:
            cur.close()


class ClickHouseConnection:
    def __init__(self, config: dict):
        self.config = config
        self._client = None
    
    def connect(self):
        from clickhouse_driver import Client
        self._client = Client(
            host=self.config.get('host', 'clickhouse'),
            port=self.config.get('port', 9000),
            database=self.config.get('database', 'default'),
            connect_timeout=30,
            send_receive_timeout=300,
        )
        logger.info(f"Подключено к ClickHouse: {self.config.get('host')}")
        return self._client
    
    def execute(self, query: str, data=None):
        if data:
            return self._client.execute(query, data)
        return self._client.execute(query)
    
    def close(self):
        if self._client:
            self._client.disconnect()
            self._client = None


@contextmanager
def source_connection(config: dict) -> Generator[SourceConnection, None, None]:
    conn = SourceConnection(config)
    try:
        conn.connect()
        yield conn
    finally:
        conn.close()


@contextmanager
def clickhouse_connection(config: dict) -> Generator[ClickHouseConnection, None, None]:
    conn = ClickHouseConnection(config)
    try:
        conn.connect()
        yield conn
    finally:
        conn.close()
