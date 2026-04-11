# Dingding Machine - 钉钉智能数据机器人

> 🚀 **正在升级为智能 Data Agent 系统**  
> 版本：v2.0.0 | 状态：第一阶段已完成

一个基于钉钉开放平台的智能数据机器人，支持自然语言交互、多数据源查询和自动化报表生成。

## 🎯 项目概述

Dingding Machine 正在从传统的定时任务系统升级为智能 Data Agent 系统：

### 传统功能（v1.x）
- ✅ 基于 cron 表达式的定时任务调度
- ✅ 从 Oracle 数据库查询数据，生成 Excel 报表
- ✅ 将报表/消息/卡片推送到指定钉钉群聊
- ✅ 支持三种任务类型：file、msg、card

### 智能化升级（v2.0）
- 🆕 **对话式数据交互** - 用户在钉钉群内用自然语言提问
- 🆕 **多数据源支持** - Oracle + PostgreSQL + Doris + REST API
- 🆕 **智能分析** - Agent 自主处理复杂分析任务
- 🆕 **自然语言任务管理** - 通过对话创建定时任务

## ✨ 核心特性

### 数据源抽象层（已完成 ✅）
- ✅ **统一接口** - 所有数据源实现相同的抽象接口
- ✅ **连接池管理** - 高效的数据库连接池
- ✅ **异步支持** - 基于 async/await 的高性能架构
- ✅ **SQL 安全** - 自动检测 SQL 注入风险
- ✅ **方言处理** - 自动处理不同数据库的 SQL 差异

### Agent 核心能力（开发中 🚧）
- 🔄 **LLM 路由** - 混合模型方案（通义千问 + DeepSeek + 本地模型）
- 🔄 **意图识别** - 自动识别用户查询意图
- 🔄 **任务编排** - 多步骤任务的自动规划和执行
- 🔄 **工具调用** - SQL 查询、数据分析、报表生成等工具

### 传统任务系统（稳定运行 ✅）
- ✅ **自动任务调度** - 基于 cron 表达式的定时任务执行
- ✅ **多格式报表** - 支持 Excel 报表生成和推送
- ✅ **钉钉集成** - 与钉钉群聊无缝集成
- ✅ **任务管理** - 自动注册和任务实例管理

## 📦 项目结构

```
dingding_machine/
├── src/                         # 源代码目录
│   ├── agent/                   # Agent 核心模块（新增）
│   │   ├── orchestrator.py      # Agent 编排器
│   │   ├── llm_router.py        # LLM 路由器
│   │   ├── conversation/        # 对话管理
│   │   └── tools/               # Agent 工具
│   │
│   ├── datasource/              # 数据源抽象层（已完成 ✅）
│   │   ├── base.py              # 数据源抽象基类
│   │   ├── exceptions.py        # 异常定义
│   │   ├── dialect.py           # SQL 方言处理
│   │   ├── registry.py          # 数据源注册中心
│   │   └── adapters/            # 数据源适配器
│   │       ├── oracle_adapter.py
│   │       ├── postgresql_adapter.py
│   │       └── doris_adapter.py
│   │
│   ├── task/                    # 任务模块（稳定）
│   │   ├── Task.py              # 任务基类
│   │   ├── airline_revenue_report_task.py
│   │   └── ...
│   │
│   ├── executor/                # 执行器模块
│   │   ├── Executor.py
│   │   ├── schedule_executor.py
│   │   └── agent_executor.py    # Agent 执行器（新增）
│   │
│   ├── handler/                 # 消息处理器
│   │   ├── dingtalk_message_handler.py
│   │   └── task_result_sender.py
│   │
│   ├── security/                # 安全模块（新增）
│   │   ├── sql_validator.py
│   │   └── audit.py
│   │
│   └── util/                    # 工具模块
│       ├── oracle_connect.py
│       ├── excel_writer.py
│       └── sql_import.py
│
├── resources/                   # 资源文件目录
│   ├── docs/                    # 项目文档（新增）
│   │   ├── README.md            # 文档导航
│   │   ├── development_plan.md  # 开发方案
│   │   ├── quick_start.md       # 快速开始
│   │   └── datasource_usage.md  # 数据源使用指南
│   │
│   ├── config/                  # 配置文件目录（新增）
│   │   ├── datasources.yaml     # 数据源配置
│   │   ├── llm.yaml             # LLM 配置（待添加）
│   │   └── prompts/             # Prompt 模板（待添加）
│   │
│   ├── skills/                  # Skill 模块目录（新增）
│   │
│   ├── excel_template/          # Excel 模板
│   └── excels/                  # 生成的 Excel 文件
│
├── tests/                       # 测试目录
│   └── test_datasource.py
│
├── main.py                      # 主入口
├── setup.py                     # 安装配置
└── requirements.txt             # 依赖列表
```

## 📂 目录约定

- **源代码**：`src/` - 所有 Python 源代码
- **项目文档**：`resources/docs/` - 所有项目文档
- **配置文件**：`resources/config/` - 所有配置文件
- **Skill 模块**：`resources/skills/` - Agent Skill 模块
- **资源文件**：`resources/` - Excel 模板、生成的文件等

## 🚀 快速开始

### 环境要求

- Python 3.10+
- 钉钉开发者账号和应用配置
- 数据库连接（Oracle/PostgreSQL/Doris）

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置数据源

编辑 `resources/config/datasources.yaml`：

```yaml
data_sources:
  oracle_main:
    type: oracle
    host: 192.168.1.73
    port: 1521
    service_name: ORCL
    user: AIR_SJ_JY
    password: ${ORACLE_PASSWORD}  # 从环境变量读取
    pool_min: 2
    pool_max: 10
    default: true
```

### 配置钉钉应用

编辑 `src/config.yaml`：

```yaml
User:
  client_id: your_client_id
  client_secret: your_client_secret

chat_group:
  - chat_name: 群聊名称
    chat_id: chatxxxxxxxxxxxxxxxx
    openSpaceId: cidxxxxxxxxxxxxxxxx
```

### 运行应用

```bash
python main.py
```

## 📖 使用指南

### 数据源抽象层使用

```python
import asyncio
from src.datasource import DataSourceRegistry

async def query_data():
    # 从配置文件加载数据源
    registry = DataSourceRegistry.from_yaml('resources/config/datasources.yaml')
    
    # 获取默认数据源
    ds = registry.get()
    
    # 执行查询
    result = await ds.query(
        "SELECT * FROM users WHERE created_at >= :start_date",
        {"start_date": "2024-01-01"}
    )
    
    print(f"查询到 {result.row_count} 行数据")
    print(f"执行时间: {result.execution_time:.2f}秒")
    print(result.data)

asyncio.run(query_data())
```

### 传统任务开发

```python
from src.task.Task import Task, task_config

@task_config(
    name="示例任务",
    ex_time="0 9 * * *",  # 每天 9:00
    task_type="file",
    chat_id="chatxxxxxxxxxxxxxxxx"
)
class ExampleTask(Task):
    def __init__(self):
        super().__init__('示例任务')
    
    def execute_task(self, conn, **kwargs):
        # 任务执行逻辑
        pass
```

## 📊 开发进度

### 第一阶段：基础设施搭建（已完成 ✅）

- ✅ 数据源抽象基类和异常体系
- ✅ Oracle 适配器
- ✅ PostgreSQL 适配器
- ✅ Doris 适配器
- ✅ 数据源注册中心
- ✅ SQL 方言处理和安全检查
- ✅ 配置文件和使用文档

### 第二阶段：Agent 核心能力开发（进行中 🚧）

- 🔄 集成 LangChain 和 LangGraph
- 🔄 实现 LLM Router
- 🔄 实现意图识别模块
- 🔄 实现 Agent 编排器
- 🔄 实现对话管理器
- 🔄 实现 SQL 查询工具
- 🔄 实现数据分析工具

### 第三阶段：集成与增强（待开始）

- ⏳ 重构钉钉消息处理器
- ⏳ 实现智能定时任务
- ⏳ 实现 SQL 安全验证
- ⏳ 实现审计日志

### 第四阶段：测试与部署（待开始）

- ⏳ 编写集成测试
- ⏳ 性能优化
- ⏳ Docker 部署

## 📚 文档

- [文档导航](resources/docs/README.md) - 完整的文档导航
- [开发方案](resources/docs/development_plan.md) - 完整的架构设计和技术选型
- [快速开始](resources/docs/quick_start.md) - 快速上手指南
- [数据源使用指南](resources/docs/datasource_usage.md) - 数据源抽象层详细说明

## 🧪 测试

```bash
# 测试数据源连接
python tests/test_datasource.py

# 运行单元测试
pytest tests/
```

## 🔧 故障排除

### 常见问题

1. **数据源连接失败**
   - 检查 `resources/config/datasources.yaml` 配置
   - 确认环境变量已正确设置
   - 查看日志 `logs/machine.log`

2. **钉钉认证失败**
   - 检查 `src/config.yaml` 中的 client_id 和 client_secret
   - 确认应用权限配置正确

3. **SQL 查询被拒绝**
   - 检查 SQL 语句是否存在注入风险
   - 使用参数化查询代替字符串拼接

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 📝 更新日志

### v2.0.0 (2026-04-11)
- 🎉 完成数据源抽象层设计和实现
- ✅ 实现 Oracle、PostgreSQL、Doris 适配器
- ✅ 实现数据源注册中心
- ✅ 实现 SQL 方言处理和安全检查
- 📚 完善项目文档体系
- 📁 规范化项目目录结构

### v1.x
- ✅ 基础定时任务系统
- ✅ 钉钉集成
- ✅ Excel 报表生成
- ✅ 任务自动注册机制

## 📄 许可证

本项目基于 MIT 许可证开源。

## 📞 联系方式

如有问题或建议，请联系项目维护者。

---

**注意**: 本项目为企业内部使用，部分配置和功能可能需要根据实际环境进行调整。
