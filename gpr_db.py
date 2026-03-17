"""
gpr_db.py
Общий пул соединений PostgreSQL для всего модуля ГПР
"""

from __future__ import annotations

from typing import Optional
import psycopg2


_db_pool: Optional[psycopg2.pool.AbstractConnectionPool] = None


def set_db_pool(pool):
    """
    Устанавливает пул соединений.
    Вызывается один раз из main_app.
    """
    global _db_pool
    _db_pool = pool


def get_conn():
    """
    Получить соединение из пула.
    """
    if not _db_pool:
        raise RuntimeError("GPR DB pool not set. Call set_db_pool() first.")
    return _db_pool.getconn()


def release_conn(conn):
    """
    Вернуть соединение обратно в пул.
    """
    if _db_pool and conn:
        _db_pool.putconn(conn)
