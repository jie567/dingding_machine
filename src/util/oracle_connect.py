import logging
import cx_Oracle
import pandas as pd
from typing import List, Any, Optional
logger = logging.getLogger(__name__)

class OracleDataConn(object):
    """Oracle数据库连接类"""

    def __init__(self, username: str = 'AIR_SJ_JY', password: str = 'AIR_SJ_JY1',
                 dsn: str = '192.168.1.73:1521/ORCL', connection_string: str = None):
        self.conn = None
        self.cursor = None
        self._username = username
        self._password = password
        self._dsn = dsn
        self._connection_string = connection_string

        try:
            self._connect()
        except Exception as e:
            print(f"数据库连接失败: {e}")
            logger.error(f"数据库连接失败: {e}")

    def _connect(self):
        if self._connection_string:
            self.conn = cx_Oracle.connect(self._connection_string)
        elif self._username and self._password and self._dsn:
            self.conn = cx_Oracle.connect(user=self._username, password=self._password, dsn=self._dsn)
        else:
            raise ValueError("必须提供连接字符串或用户名/密码/DSN")
        self.cursor = self.conn.cursor()

    def _ensure_connection(self):
        try:
            self.cursor.execute("SELECT 1 FROM DUAL")
            self.cursor.fetchone()
        except Exception:
            logger.warning("检测到数据库连接已断开，正在重新连接...")
            self._reconnect()

    def _reconnect(self):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        try:
            self._connect()
            logger.info("数据库重新连接成功")
        except Exception as e:
            logger.error(f"数据库重新连接失败: {e}")
            raise

    def __enter__(self):
        """支持上下文管理器"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时自动关闭连接"""
        self.close()

    def query_as_df(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        执行查询并返回DataFrame

        Args:
            sql: SQL查询语句
            params: 查询参数

        Returns:
            pandas DataFrame
        """
        self._ensure_connection()
        try:
            self.cursor.execute(sql, params or ())
            data = self.cursor.fetchall()
            col_names = [desc[0] for desc in self.cursor.description]
            df = pd.DataFrame(data, columns=col_names)
            return df

        except Exception as e:
            self.conn.rollback()
            logger.error(f'查询失败: {e}')
            print(f'查询失败: {e}')

    def batch_insert(self, sql: str, data) -> int:
        """
        批量插入数据

        Args:
            sql: INSERT语句  示例：sql = "INSERT INTO table_name (column1, column2) VALUES (:1, :2)"
            data: 要插入的数据列表

        Returns:
            插入的行数
        """
        self._ensure_connection()
        try:
            if isinstance(data, pd.DataFrame):
                data = [tuple(x) for x in data.to_numpy()]
            elif not isinstance(data, List):
                raise Exception('批量插入失败: data 类型不是列表类型或者dataframe类型')

            self.cursor.executemany(sql, data, batcherrors=False)
            errors = self.cursor.getbatcherrors()
            if errors:
                for error in errors:
                    print(f"批量插入错误 - 偏移量 {error.offset}: {error.message}")
                self.conn.rollback()
                raise Exception(errors)
            else:
                self.conn.commit()
                print(f"批量插入成功{self.cursor.rowcount}条")
                return self.cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            logger.error(f'批量插入失败: {e.__traceback__}')
            print(f'批量插入失败: {e.__traceback__}')

    def execute(self, sql: str, params: Optional[tuple] = None) -> int:
        """执行单条SQL语句"""
        self._ensure_connection()
        try:
            self.cursor.execute(sql, params or ())
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f'执行单条语句失败: {e}')
            print(f'执行单条语句失败: {e}')

    def close(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("数据库连接已关闭")

    # 析构函数作为备份
    def __del__(self):
        self.close()



if __name__ == "__main__":
    config = {
        'username': 'AIR_SJ_JY',
        'password': 'AIR_SJ_JY1',
        'dsn': '192.168.1.73:1521/ORCL'
    }

    # with OracleDataConn(**config) as db:
    #     df = db.query_as_df("SELECT * FROM air1.TB_FOC_T1011D  ")
    #     print(df.head())

    db = OracleDataConn()
    try:
        df = db.query_as_df("SELECT * FROM air1.TB_FOC_T1011D where rownum < 100 ")
        print(df.head())
    except Exception as e:
        print(e)