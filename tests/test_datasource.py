"""
数据源抽象层测试示例

演示如何使用数据源抽象层进行数据库操作
"""
import asyncio
from src.datasource import DataSourceRegistry, QueryResult


async def test_datasource():
    """测试数据源功能"""
    
    # 1. 从配置文件加载数据源
    registry = DataSourceRegistry.from_yaml("resources/config/datasources.yaml")
    
    print("已注册的数据源:", registry.list_sources())
    
    # 2. 获取默认数据源（Oracle）
    oracle_ds = registry.get()
    print(f"默认数据源: {oracle_ds.config.name}")
    
    # 3. 测试连接
    is_connected = await oracle_ds.test_connection()
    print(f"连接状态: {'成功' if is_connected else '失败'}")
    
    # 4. 执行查询
    try:
        result: QueryResult = await oracle_ds.query(
            "SELECT * FROM user_tables WHERE rownum <= 5"
        )
        
        print(f"\n查询成功！")
        print(f"行数: {result.row_count}")
        print(f"列: {result.columns}")
        print(f"执行时间: {result.execution_time:.2f}秒")
        print(f"\n数据预览:")
        print(result.data.head())
        
    except Exception as e:
        print(f"查询失败: {e}")
    
    # 5. 获取表结构
    try:
        schema = await oracle_ds.get_schema("TB_FOC_T1011D")
        print(f"\n表结构: {schema.table_name}")
        print(f"列数: {len(schema.columns)}")
        print(f"主键: {schema.primary_keys}")
        
    except Exception as e:
        print(f"获取表结构失败: {e}")
    
    # 6. 健康检查
    health_status = await registry.health_check()
    print(f"\n健康检查结果: {health_status}")


if __name__ == "__main__":
    asyncio.run(test_datasource())
