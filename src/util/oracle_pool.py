import logging
import cx_Oracle
import pandas as pd
import threading
from typing import List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class OracleConnectionPool:
    """Oracle数据库连接池类 - 基于 cx_Oracle.SessionPool"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, username: str = 'AIR_SJ_JY', password: str = 'AIR_SJ_JY1',
                 dsn: str = '192.168.1.73:1521/ORCL',
                 min_connections: int = 2,
                 max_connections: int = 8,
                 increment: int = 1,
                 threaded: bool = True,
                 encoding: str = 'UTF-8',
                 getmode: int = cx_Oracle.SPOOL_ATTRVAL_WAIT):
        if hasattr(self, '_initialized') and self._initialized:
            return

        self._username = username
        self._password = password
        self._dsn = dsn
        self._min = min_connections
        self._max = max_connections
        self._increment = increment
        self._threaded = threaded
        self._encoding = encoding
        self._getmode = getmode
        self._pool = None
        self._initialized = False
        self._rebuild_condition = threading.Condition()
        self._rebuilding = False

        self._init_pool()

    def _init_pool(self):
        try:
            self._pool = cx_Oracle.SessionPool(
                user=self._username,
                password=self._password,
                dsn=self._dsn,
                min=self._min,
                max=self._max,
                increment=self._increment,
                threaded=self._threaded,
                encoding=self._encoding,
                getmode=self._getmode
            )
            self._initialized = True
            logger.info(f"数据库连接池初始化成功: min={self._min}, max={self._max}")
        except Exception as e:
            logger.error(f"数据库连接池初始化失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self._pool.acquire()
            yield conn
        except cx_Oracle.DatabaseError as e:
            error_obj = e.args[0] if e.args else None
            error_code = getattr(error_obj, 'code', None) if error_obj else None

            if error_code in (3113, 3114, 1080, 1010):
                error_msg = getattr(error_obj, 'message', str(e))
                logger.warning(f"连接已断开，尝试重建连接池: {error_msg}")
                self._rebuild_pool()
                conn = self._pool.acquire()
                yield conn
            else:
                raise
        finally:
            if conn is not None:
                try:
                    self._pool.release(conn)
                except Exception as e:
                    logger.error(f"连接池释放连接不成功: {e}")

    def _rebuild_pool(self):
        with self._rebuild_condition:
            if self._rebuilding:
                self._rebuild_condition.wait_for(lambda: not self._rebuilding, timeout=30)
                return
            self._rebuilding = True

        try:
            old_pool = self._pool
            new_pool = cx_Oracle.SessionPool(
                user=self._username,
                password=self._password,
                dsn=self._dsn,
                min=self._min,
                max=self._max,
                increment=self._increment,
                threaded=self._threaded,
                encoding=self._encoding,
                getmode=self._getmode
            )

            with self._rebuild_condition:
                self._pool = new_pool
                self._rebuilding = False
                self._rebuild_condition.notify_all()

            logger.info("连接池重建成功")

            if old_pool is not None:
                try:
                    old_pool.close()
                except Exception as e:
                    logger.warning(f"关闭旧连接池时出错: {e}")

        except Exception as e:
            with self._rebuild_condition:
                self._rebuilding = False
                self._rebuild_condition.notify_all()
            logger.error(f"连接池重建失败: {e}")
            raise

    def query_as_df(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or ())
                data = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(data, columns=col_names)
                return df
            except Exception as e:
                conn.rollback()
                logger.error(f'查询失败: {e}')
                raise
            finally:
                cursor.close()

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or ())
                conn.commit()
                return cursor.rowcount
            except Exception as e:
                conn.rollback()
                logger.error(f'执行SQL失败: {e}')
                raise
            finally:
                cursor.close()

    def batch_insert(self, sql: str, data) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if isinstance(data, pd.DataFrame):
                    data = [tuple(x) for x in data.to_numpy()]
                elif not isinstance(data, List):
                    raise Exception('批量插入失败: data 类型不是列表类型或者dataframe类型')

                cursor.executemany(sql, data, batcherrors=False)
                errors = cursor.getbatcherrors()
                if errors:
                    for error in errors:
                        logger.error(f"批量插入错误 - 偏移量 {error.offset}: {error.message}")
                    conn.rollback()
                    raise Exception(errors)
                else:
                    conn.commit()
                    logger.info(f"批量插入成功{cursor.rowcount}条")
                    return cursor.rowcount
            except Exception as e:
                conn.rollback()
                logger.error(f'批量插入失败: {e}')
                raise
            finally:
                cursor.close()

    def close(self):
        if self._pool is not None:
            try:
                self._pool.close()
                logger.info("数据库连接池已关闭")
            except Exception as e:
                logger.error(f"关闭连接池失败: {e}")
        self._initialized = False

    def __del__(self):
        self.close()

    @property
    def pool_status(self) -> dict:
        if self._pool is None:
            return {'status': 'not_initialized'}
        return {
            'status': 'active',
            'opened': self._pool.opened,
            'busy': self._pool.busy,
            'max': self._max,
            'min': self._min
        }


class PooledConnection:
    """连接池包装器 - 提供与 OracleDataConn 相同的接口，使用全局单例连接池"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, pool: OracleConnectionPool = None, **pool_kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, pool: OracleConnectionPool = None, **pool_kwargs):
        if self._initialized:
            return

        if pool is not None:
            self._pool = pool
        else:
            self._pool = OracleConnectionPool(**pool_kwargs)
        self._initialized = True

    def query_as_df(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        return self._pool.query_as_df(sql, params)

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        return self._pool.execute(sql, params)

    def batch_insert(self, sql: str, data) -> int:
        return self._pool.batch_insert(sql, data)

    def close(self):
        self._pool.close()

    @property
    def pool_status(self) -> dict:
        return self._pool.pool_status


