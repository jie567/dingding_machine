"""
SQL 方言处理工具

处理不同数据库的 SQL 方言差异，提供 SQL 安全检查功能
"""
import re
import sqlparse
from .exceptions import SQLInjectionRiskException


class DialectHelper:
    """SQL 方言处理工具"""
    
    DANGEROUS_PATTERNS = [
        r';.*(?:DROP|DELETE|TRUNCATE|ALTER|CREATE)',
        r'--',
        r'/\*',
        r'xp_',
        r'EXEC\s+',
        r'UNION\s+SELECT',
    ]
    
    def __init__(self, dialect: str):
        """
        初始化方言处理器
        
        Args:
            dialect: 数据库方言类型 (oracle, postgresql, doris, mysql)
        """
        self.dialect = dialect.lower()
    
    def validate_sql(self, sql: str) -> bool:
        """
        SQL 安全检查
        
        Args:
            sql: SQL 语句
            
        Returns:
            bool: 是否安全
            
        Raises:
            SQLInjectionRiskException: 检测到 SQL 注入风险
        """
        sql_no_strings = re.sub(r"'[^']*'", "", sql)
        sql_no_strings = re.sub(r'"[^"]*"', "", sql_no_strings)
        
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_no_strings, re.IGNORECASE):
                raise SQLInjectionRiskException(
                    f"检测到潜在的 SQL 注入风险: {pattern}"
                )
        
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            if statement.get_type() in ('DROP', 'DELETE', 'TRUNCATE', 'ALTER'):
                pass
        
        return True
    
    def add_limit(self, sql: str, limit: int) -> str:
        """
        添加结果集限制
        
        Args:
            sql: SQL 语句
            limit: 限制行数
            
        Returns:
            str: 添加了限制的 SQL
        """
        if self.dialect in ["oracle"]:
            return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {limit}"
        elif self.dialect in ["postgresql", "doris", "mysql"]:
            return f"{sql} LIMIT {limit}"
        else:
            return sql
    
    def format_date(self, date_expr: str) -> str:
        """
        格式化日期表达式
        
        Args:
            date_expr: 日期表达式 (YYYY-MM-DD)
            
        Returns:
            str: 格式化后的日期表达式
        """
        if self.dialect == "oracle":
            return f"TO_DATE('{date_expr}', 'YYYY-MM-DD')"
        elif self.dialect in ["postgresql", "doris"]:
            return f"'{date_expr}'::date"
        else:
            return f"'{date_expr}'"
    
    def get_current_date(self) -> str:
        """
        获取当前日期函数
        
        Returns:
            str: 当前日期函数
        """
        if self.dialect == "oracle":
            return "SYSDATE"
        elif self.dialect == "postgresql":
            return "CURRENT_DATE"
        elif self.dialect == "doris":
            return "CURRENT_DATE()"
        else:
            return "NOW()"
    
    def get_pagination(self, sql: str, offset: int, limit: int) -> str:
        """
        生成分页查询
        
        Args:
            sql: SQL 语句
            offset: 偏移量
            limit: 每页数量
            
        Returns:
            str: 分页 SQL
        """
        if self.dialect == "oracle":
            return f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rn FROM ({sql}) a 
                WHERE ROWNUM <= {offset + limit}
            ) WHERE rn > {offset}
            """
        elif self.dialect in ["postgresql", "doris"]:
            return f"{sql} LIMIT {limit} OFFSET {offset}"
        else:
            return f"{sql} LIMIT {limit} OFFSET {offset}"
    
    def get_string_concat(self, *args: str) -> str:
        """
        获取字符串连接表达式
        
        Args:
            *args: 要连接的字符串
            
        Returns:
            str: 字符串连接表达式
        """
        if self.dialect == "oracle":
            return " || ".join(args)
        elif self.dialect == "postgresql":
            return " || ".join(args)
        else:
            return "CONCAT(" + ", ".join(args) + ")"
    
    def get_if_null(self, expr: str, default: str) -> str:
        """
        获取空值处理函数
        
        Args:
            expr: 表达式
            default: 默认值
            
        Returns:
            str: 空值处理表达式
        """
        if self.dialect == "oracle":
            return f"NVL({expr}, {default})"
        elif self.dialect in ["postgresql", "doris"]:
            return f"COALESCE({expr}, {default})"
        else:
            return f"IFNULL({expr}, {default})"
