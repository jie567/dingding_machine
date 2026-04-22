"""
Agent 核心功能测试

测试 Agent 的各个组件：
1. 意图识别
2. 对话管理
3. 工具调用
4. 完整流程
"""

import asyncio
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agent import (
    IntentRecognizer,
    IntentType,
    ConversationManager,
    ToolRegistry,
    ToolResult
)
from src.agent.tools.sql_tool import SQLQueryTool, SchemaExplorerTool
from src.agent.tools.analysis_tool import DataAnalysisTool
from src.agent.tools.excel_tool import ExcelGeneratorTool


async def test_intent_recognition():
    """测试意图识别"""
    print("\n" + "="*50)
    print("🧪 测试意图识别")
    print("="*50)
    
    recognizer = IntentRecognizer()
    
    test_cases = [
        ("查一下昨天的航线收益", IntentType.DATA_QUERY),
        ("分析一下本月和上月的 CPA 利润对比", IntentType.DATA_ANALYSIS),
        ("生成一个 Excel 报表", IntentType.REPORT_GENERATION),
        ("每天早上 9 点发送日报", IntentType.TASK_MANAGEMENT),
        ("你能做什么", IntentType.HELP),
        ("你好", IntentType.GREETING),
        ("这个是什么意思", IntentType.CLARIFICATION),
    ]
    
    for text, expected_intent in test_cases:
        result = recognizer.recognize(text)
        
        status = "✅" if result.intent == expected_intent else "❌"
        print(f"\n{status} 输入: {text}")
        print(f"   预期: {expected_intent.value}")
        print(f"   实际: {result.intent.value} (置信度: {result.confidence:.2f})")
        print(f"   实体: {result.entities}")


async def test_conversation_manager():
    """测试对话管理器"""
    print("\n" + "="*50)
    print("🧪 测试对话管理器")
    print("="*50)
    
    manager = ConversationManager()
    
    # 1. 创建会话
    print("\n1. 创建会话")
    session = manager.create_session("chat_123", "user_456")
    print(f"✅ 会话创建成功: {session.session_id[:8]}...")
    
    # 2. 添加消息
    print("\n2. 添加消息")
    manager.add_message(session.session_id, "user", "查一下昨天的数据")
    manager.add_message(session.session_id, "assistant", "好的，正在查询...")
    manager.add_message(session.session_id, "user", "再分析一下趋势")
    
    print(f"✅ 已添加 {len(session.messages)} 条消息")
    
    # 3. 获取最近消息
    print("\n3. 获取最近消息")
    recent = session.get_recent_messages(2)
    for msg in recent:
        print(f"   [{msg.role}]: {msg.content}")
    
    # 4. 获取 LLM 格式消息
    print("\n4. 获取 LLM 格式消息")
    llm_messages = session.get_messages_for_llm()
    for msg in llm_messages:
        print(f"   [{msg['role']}]: {msg['content'][:30]}...")
    
    # 5. 获取会话统计
    print("\n5. 会话统计")
    stats = manager.get_stats()
    print(f"   总会话: {stats['total_sessions']}")
    print(f"   活跃会话: {stats['active_sessions']}")
    print(f"   总消息: {stats['total_messages']}")


async def test_tools():
    """测试工具"""
    print("\n" + "="*50)
    print("🧪 测试工具")
    print("="*50)
    
    registry = ToolRegistry()
    
    # 1. 注册工具
    print("\n1. 注册工具")
    registry.register(SQLQueryTool())
    registry.register(SchemaExplorerTool())
    registry.register(DataAnalysisTool())
    registry.register(ExcelGeneratorTool())
    
    print(f"✅ 已注册 {len(registry.list_tools())} 个工具")
    print(f"   工具列表: {', '.join(registry.list_tools())}")
    
    # 2. 获取工具描述
    print("\n2. 工具描述")
    descriptions = registry.get_tool_descriptions()
    print(descriptions)
    
    # 3. 测试数据分析工具（不需要数据库）
    print("\n3. 测试数据分析工具")
    
    # 准备测试数据
    import json
    test_data = [
        {"name": "A", "value": 100, "category": "X"},
        {"name": "B", "value": 200, "category": "Y"},
        {"name": "C", "value": 150, "category": "X"},
        {"name": "D", "value": 300, "category": "Y"},
        {"name": "E", "value": 250, "category": "X"}
    ]
    
    # 测试统计摘要
    result = await registry.execute(
        "data_analysis",
        data_json=json.dumps(test_data),
        analysis_type="summary"
    )
    
    print(f"\n   统计摘要:")
    print(f"   成功: {result.success}")
    print(f"   消息: {result.message}")
    if result.data:
        print(f"   行数: {result.data.get('total_rows')}")
        print(f"   列数: {result.data.get('total_columns')}")
    
    # 测试分组统计
    result = await registry.execute(
        "data_analysis",
        data_json=json.dumps(test_data),
        analysis_type="groupby",
        group_by="category"
    )
    
    print(f"\n   分组统计:")
    print(f"   成功: {result.success}")
    print(f"   消息: {result.message}")
    
    # 4. 获取工具统计
    print("\n4. 工具统计")
    stats = registry.get_all_stats()
    for tool_name, tool_stats in stats.items():
        print(f"   {tool_name}:")
        print(f"     执行次数: {tool_stats['execution_count']}")
        print(f"     成功率: {tool_stats['success_rate']:.2%}")


async def test_full_flow():
    """测试完整流程"""
    print("\n" + "="*50)
    print("🧪 测试完整流程（模拟）")
    print("="*50)
    
    from src.agent import AgentOrchestrator
    
    # 创建编排器（不连接 LLM）
    orchestrator = AgentOrchestrator()
    
    # 测试问候
    print("\n1. 测试问候")
    response = await orchestrator.process_message(
        user_input="你好",
        chat_id="test_chat",
        user_id="test_user"
    )
    print(f"   用户: 你好")
    print(f"   助手: {response.message}")
    
    # 测试帮助
    print("\n2. 测试帮助")
    response = await orchestrator.process_message(
        user_input="你能做什么",
        chat_id="test_chat",
        user_id="test_user"
    )
    print(f"   用户: 你能做什么")
    print(f"   助手: {response.message[:100]}...")
    
    # 测试数据查询（需要数据库连接，可能会失败）
    print("\n3. 测试数据查询")
    try:
        response = await orchestrator.process_message(
            user_input="查一下昨天的航线收益",
            chat_id="test_chat",
            user_id="test_user"
        )
        print(f"   用户: 查一下昨天的航线收益")
        print(f"   助手: {response.message[:100]}...")
        print(f"   成功: {response.success}")
    except Exception as e:
        print(f"   ⚠️  数据查询测试跳过（需要数据库连接）: {e}")
    
    # 获取统计
    print("\n4. 统计信息")
    stats = orchestrator.get_stats()
    print(f"   会话统计: {stats['conversation_stats']}")
    print(f"   注册工具: {stats['registered_tools']}")


async def main():
    """主测试函数"""
    print("\n" + "🚀" * 25)
    print("开始 Agent 核心功能测试")
    print("🚀" * 25)
    
    try:
        # 1. 测试意图识别
        await test_intent_recognition()
        
        # 2. 测试对话管理
        await test_conversation_manager()
        
        # 3. 测试工具
        await test_tools()
        
        # 4. 测试完整流程
        await test_full_flow()
        
        print("\n" + "✅" * 25)
        print("所有测试完成！")
        print("✅" * 25)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
