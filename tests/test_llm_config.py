"""
LLM 配置测试

测试 LLM Router 是否能正确加载配置并调用模型。
"""

import asyncio
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agent.config_loader import init_llm_router_from_config


async def test_llm_connection():
    """测试 LLM 连接"""
    print("\n" + "="*50)
    print("🧪 测试 LLM 配置和连接")
    print("="*50)
    
    # 1. 初始化 LLM Router
    print("\n1. 初始化 LLM Router")
    router = init_llm_router_from_config("src/config.yaml")
    
    providers = router.list_providers()
    print(f"✅ 已注册 {len(providers)} 个提供商: {', '.join(providers)}")
    
    if not providers:
        print("❌ 没有可用的 LLM 提供商，请检查配置")
        return
    
    # 2. 健康检查
    print("\n2. 健康检查")
    health_status = await router.health_check()
    
    for name, is_healthy in health_status.items():
        status = "✅ 正常" if is_healthy else "❌ 异常"
        print(f"   {name}: {status}")
    
    # 3. 测试简单对话
    print("\n3. 测试简单对话")
    
    messages = [
        {"role": "system", "content": "你是一个 helpful assistant。"},
        {"role": "user", "content": "你好，请简单介绍一下自己"}
    ]
    
    try:
        # 测试主提供商
        if "通义千问" in providers:
            print("\n   测试通义千问...")
            response = await router.chat(messages, provider="通义千问")
            print(f"   ✅ 通义千问响应: {response[:100]}...")
        
        # 测试备用提供商
        if "DeepSeek" in providers:
            print("\n   测试 DeepSeek...")
            response = await router.chat(messages, provider="DeepSeek")
            print(f"   ✅ DeepSeek 响应: {response[:100]}...")
        
        # 测试自动选择
        print("\n   测试自动选择...")
        response = await router.chat(messages)
        print(f"   ✅ 自动选择响应: {response[:100]}...")
        
    except Exception as e:
        print(f"   ❌ 对话测试失败: {e}")
    
    # 4. 获取统计信息
    print("\n4. 统计信息")
    health_stats = router.get_health_status()
    
    for name, status in health_stats.items():
        print(f"   {name}:")
        print(f"     状态: {status.status.value}")
        print(f"     成功率: {status.success_rate:.2%}")
        print(f"     平均响应时间: {status.avg_response_time:.2f}秒")
        print(f"     成功次数: {status.success_count}")
        print(f"     错误次数: {status.error_count}")


async def test_sql_generation():
    """测试 SQL 生成"""
    print("\n" + "="*50)
    print("🧪 测试 SQL 生成")
    print("="*50)
    
    from src.agent import AgentOrchestrator
    
    orchestrator = AgentOrchestrator()
    
    # 测试生成 SQL
    test_queries = [
        "查一下昨天的航线收益",
        "统计本月 CPA 利润",
        "显示最近 7 天的航班数据"
    ]
    
    for query in test_queries:
        print(f"\n   用户: {query}")
        
        try:
            # 创建模拟会话
            from src.agent.conversation.manager import ConversationManager
            manager = ConversationManager()
            session = manager.create_session("test_chat", "test_user")
            
            # 生成 SQL
            sql = await orchestrator._generate_sql(query, session)
            
            if sql:
                print(f"   ✅ 生成 SQL: {sql[:100]}...")
            else:
                print(f"   ❌ 未能生成 SQL")
                
        except Exception as e:
            print(f"   ❌ 错误: {e}")


async def main():
    """主测试函数"""
    print("\n" + "🚀" * 25)
    print("开始 LLM 配置测试")
    print("🚀" * 25)
    
    try:
        # 1. 测试 LLM 连接
        await test_llm_connection()
        
        # 2. 测试 SQL 生成
        await test_sql_generation()
        
        print("\n" + "✅" * 25)
        print("LLM 测试完成！")
        print("✅" * 25)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
