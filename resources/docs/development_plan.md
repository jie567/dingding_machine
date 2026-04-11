# Dingding Machine 智能化升级开发方案

> 版本：v1.0  
> 日期：2026-04-11  
> 作者：AI Agent 架构师

## 目录

- [一、项目背景](#一项目背景)
- [二、升级目标](#二升级目标)
- [三、系统架构设计](#三系统架构设计)
- [四、技术选型](#四技术选型)
- [五、多数据源抽象层](#五多数据源抽象层)
- [六、Agent 工具体系](#六agent-工具体系)
- [七、对话管理设计](#七对话管理设计)
- [八、项目结构](#八项目结构)
- [九、开发路线图](#九开发路线图)
- [十、风险评估](#十风险评估)

---

## 一、项目背景

### 1.1 现有项目概况

**项目名称**：Dingding Machine（钉钉数据机器人）

**技术栈**：
- Python 3.10+
- 钉钉开放平台 API
- Oracle 数据库
- APScheduler 定时调度

**核心功能**：
- 基于 cron 表达式的定时任务调度
- 从 Oracle 数据库查询数据，生成 Excel 报表
- 将报表/消息/卡片推送到指定钉钉群聊
- 支持三种任务类型：file、msg、card

**现有架构模式**：
- Task 基类 + 子类继承的任务模式
- @task_config 装饰器自动注册任务
- Executor 执行器负责调度和执行
- 工具层：oracle_connect、excel_writer、sql_import

### 1.2 升级需求

将现有的"硬编码定时任务"模式，升级为"智能 Data Agent"系统，具备以下核心能力：

1. **对话式数据交互**：用户在钉钉群内用自然语言提问
2. **智能定时任务**：Agent 自主处理异常，支持自然语言创建任务
3. **多数据源支持**：除 Oracle 外，支持 PostgreSQL、Doris、REST API
4. **自主决策与工具调用**：Agent 能根据复杂需求自主拆解为多个子任务

---

## 二、升级目标

### 2.1 核心能力

| 能力 | 描述 | 优先级 |
|------|------|--------|
| 对话式查询 | 自然语言数据查询 | P0 |
| 多数据源支持 | Oracle + PostgreSQL + Doris | P0 |
| 智能分析 | 复杂数据分析和推理 | P1 |
| 定时任务管理 | 自然语言创建定时任务 | P1 |
| 报表生成 | 自动生成 Excel 报表 | P0 |

### 2.2 技术约束

- **语言**：Python 3.10+
- **LLM**：混合方案（通义千问 + DeepSeek + 本地模型）
- **部署**：混合部署（LLM 云端，应用内网）
- **安全**：中等敏感度，需要 SQL 注入防护和审计日志

---

## 三、系统架构设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        用户交互层                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  钉钉群聊    │  │  定时触发    │  │  手动触发    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Agent 核心层                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  LLM Router (混合模型路由)                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Agent Orchestrator (编排器)                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Conversation Manager (对话管理器)                        │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      工具调用层                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ SQL 执行工具 │  │ 数据分析工具 │  │ 报表生成工具 │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   数据源抽象层 (核心新增)                        │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ │
│  │   Oracle   │ │ PostgreSQL │ │   Doris    │ │ REST API   │ │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 核心模块职责

| 模块 | 职责 | 状态 |
|------|------|------|
| **LLM Router** | 多模型路由、负载均衡、降级策略 | 待开发 |
| **Agent Orchestrator** | 意图识别、任务规划、执行编排 | 待开发 |
| **Conversation Manager** | 多轮对话上下文、会话状态管理 | 待开发 |
| **Tool System** | 工具注册、发现、调用、结果处理 | 待开发 |
| **DataSource Layer** | 多数据源抽象、统一接口、连接管理 | ✅ 已完成 |
| **DingTalk Handler** | 钉钉消息接收、解析、发送 | 复用并增强 |
| **Task System** | 定时任务调度、执行 | 复用并增强 |

---

## 四、技术选型

### 4.1 Agent 框架对比

| 框架 | 优势 | 劣势 | 推荐度 |
|------|------|------|--------|
| **LangChain + LangGraph** | 灵活性高、社区活跃、状态管理强大 | 学习曲线中等 | ⭐⭐⭐⭐⭐ |
| AutoGen | 多 Agent 协作能力强 | 过于复杂，不适合单 Agent 场景 | ⭐⭐⭐ |
| CrewAI | 简单易用 | 框架约束多，定制灵活性不足 | ⭐⭐⭐ |
| Dify | 部署简单、可视化配置 | 平台迁移成本高，定制受限 | ⭐⭐⭐⭐ |

**最终选择**：**LangChain + LangGraph**

**理由**：
1. 需要深度集成现有 Python 代码
2. 需要完全控制 Agent 决策流程
3. 需要支持混合 LLM
4. LangGraph 提供强大的状态机能力

### 4.2 LLM 选型

**混合方案**：

```yaml
llm_providers:
  primary:
    name: "通义千问"
    model: "qwen-plus"
    use_cases: ["数据分析", "SQL 生成", "中文对话"]
    
  secondary:
    name: "DeepSeek"
    model: "deepseek-chat"
    use_cases: ["复杂推理", "降级备用"]
    
  fallback:
    name: "本地模型"
    model: "qwen2.5-7b-instruct"
    use_cases: ["网络故障降级", "敏感数据处理"]
```

### 4.3 关键依赖

```txt
# Agent 核心
langchain==0.3.0
langgraph==0.2.0
langchain-openai==0.2.0

# 数据库驱动
oracledb==2.0.0
asyncpg==0.29.0
mysql-connector-python==8.3.0

# 数据处理
pandas==2.2.0
pydantic==2.8.0

# 安全
sqlparse==0.5.0

# 现有依赖保留
apscheduler==3.11.2
dingtalk-stream==0.24.3
```

---

## 五、多数据源抽象层

### 5.1 设计理念

**统一接口**：所有数据源实现相同的抽象接口，屏蔽底层差异

**核心特性**：
- 异步支持（async/await）
- 连接池管理
- SQL 安全检查
- 方言处理
- 健康检查

### 5.2 核心组件

#### 5.2.1 数据源抽象基类

```python
class DataSource(ABC):
    @abstractmethod
    async def connect(self) -> bool: pass
    
    @abstractmethod
    async def query(self, sql: str, params=None, limit=None) -> QueryResult: pass
    
    @abstractmethod
    async def execute(self, sql: str, params=None) -> int: pass
    
    @abstractmethod
    async def test_connection(self) -> bool: pass
    
    @abstractmethod
    async def get_schema(self, table_name: str) -> TableSchema: pass
    
    @abstractmethod
    async def get_tables(self, schema=None) -> List[str]: pass
```

#### 5.2.2 统一查询结果

```python
class QueryResult(BaseModel):
    data: DataFrame          # 查询结果数据
    row_count: int          # 返回行数
    columns: List[str]      # 列名列表
    execution_time: float   # 执行耗时(秒)
    source_name: str        # 数据源名称
    query_sql: str          # 执行的 SQL
```

#### 5.2.3 数据源适配器

| 适配器 | 数据库 | 连接池 | 异步 | 状态 |
|--------|--------|--------|------|------|
| OracleAdapter | Oracle | ✅ | ✅ | ✅ 已完成 |
| PostgreSQLAdapter | PostgreSQL | ✅ | ✅ | ✅ 已完成 |
| DorisAdapter | Doris | ✅ | ✅ | ✅ 已完成 |

### 5.3 使用示例

```python
from src.datasource import DataSourceRegistry

# 从配置文件加载
registry = DataSourceRegistry.from_yaml('config/datasources.yaml')

# 获取数据源
ds = registry.get("oracle_main")

# 执行查询
result = await ds.query("SELECT * FROM users WHERE id = :id", {"id": 123})

print(f"查询到 {result.row_count} 行")
print(f"执行时间: {result.execution_time:.2f}秒")
print(result.data)
```

### 5.4 SQL 安全检查

```python
from src.datasource import DialectHelper

dialect = DialectHelper("oracle")

# 安全检查
try:
    dialect.validate_sql("SELECT * FROM users; DROP TABLE users; --")
except SQLInjectionRiskException as e:
    print(f"检测到 SQL 注入风险: {e}")
```

### 5.5 方言处理

```python
# 添加结果集限制
sql = dialect.add_limit("SELECT * FROM users", 100)

# Oracle: SELECT * FROM (SELECT * FROM users) WHERE ROWNUM <= 100
# PostgreSQL: SELECT * FROM users LIMIT 100

# 获取当前日期
date_func = dialect.get_current_date()
# Oracle: SYSDATE
# PostgreSQL: CURRENT_DATE
```

---

## 六、Agent 工具体系

### 6.1 工具清单

| 工具名称 | 功能 | 输入 | 输出 | 优先级 |
|---------|------|------|------|--------|
| SQLQueryTool | 执行 SQL 查询 | SQL、数据源 | DataFrame | P0 |
| DataAnalysisTool | 数据分析统计 | DataFrame、分析类型 | 分析结果 | P0 |
| ExcelGeneratorTool | 生成 Excel 报表 | DataFrame、模板 | 文件路径 | P0 |
| DingTalkSenderTool | 发送钉钉消息 | 消息内容、群聊 ID | 发送状态 | P0 |
| TaskSchedulerTool | 创建定时任务 | 任务配置 | 任务 ID | P1 |
| SchemaExplorerTool | 探索数据库结构 | 数据源、表名 | 表结构 | P1 |

### 6.2 工具定义示例

```python
from langchain_core.tools import tool

@tool
async def execute_sql_query(
    sql: str,
    data_source: Optional[str] = None,
    limit: int = 1000
) -> str:
    """
    执行 SQL 查询并返回结果
    
    Args:
        sql: SQL 查询语句
        data_source: 数据源名称（可选）
        limit: 返回结果行数限制
        
    Returns:
        str: 查询结果的 JSON 格式字符串
    """
    registry = DataSourceRegistry.get_instance()
    ds = registry.get(data_source)
    
    result = await ds.query(sql, limit=limit)
    return result.data.to_json(orient='records', force_ascii=False)
```

### 6.3 工具注册中心

```python
class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, BaseTool] = {}
    
    def register(self, tool: BaseTool) -> None:
        self._tools[tool.name] = tool
    
    def list_tools(self) -> List[BaseTool]:
        return list(self._tools.values())
```

---

## 七、对话管理设计

### 7.1 多轮对话状态

```python
class ConversationState(TypedDict):
    session_id: str              # 会话 ID
    chat_id: str                 # 钉钉群聊 ID
    user_id: str                 # 用户 ID
    messages: List[BaseMessage]  # 消息历史
    current_intent: Optional[str] # 当前意图
    current_task: Optional[str]   # 当前任务
    data_context: Dict[str, Any]  # 数据上下文
    tool_calls: List[Dict]        # 工具调用记录
```

### 7.2 意图识别

```python
class IntentRecognizer:
    async def recognize(self, user_input: str) -> IntentClassification:
        """
        识别用户意图
        
        意图类型：
        - data_query: 数据查询
        - data_analysis: 数据分析
        - report_generation: 报表生成
        - task_management: 任务管理
        - help: 帮助
        - clarification: 需要澄清
        """
        pass
```

### 7.3 Agent 编排器（LangGraph）

```python
class AgentOrchestrator:
    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(ConversationState)
        
        # 添加节点
        workflow.add_node("intent_recognition", self._recognize_intent)
        workflow.add_node("task_planning", self._plan_task)
        workflow.add_node("tool_execution", self._execute_tools)
        workflow.add_node("response_generation", self._generate_response)
        
        # 设置流程
        workflow.set_entry_point("intent_recognition")
        workflow.add_edge("intent_recognition", "task_planning")
        workflow.add_edge("task_planning", "tool_execution")
        workflow.add_edge("tool_execution", "response_generation")
        workflow.add_edge("response_generation", END)
        
        return workflow.compile()
```

### 7.4 自然语言定时任务

```python
class NaturalLanguageTaskParser:
    async def parse(self, user_input: str) -> TaskConfig:
        """
        解析自然语言任务描述
        
        示例：
        - "每天早上 9 点" -> "0 9 * * *"
        - "每周一 9:30" -> "30 9 * * 1"
        - "每月最后一天 10:00" -> "0 10 28-31 * *"
        """
        pass
```

---

## 八、项目结构

```
dingding_machine/
├── src/
│   ├── agent/                    # Agent 核心模块（新增）
│   │   ├── orchestrator.py       # Agent 编排器
│   │   ├── llm_router.py         # LLM 路由器
│   │   ├── conversation/         # 对话管理
│   │   └── tools/                # Agent 工具
│   │
│   ├── datasource/               # 数据源抽象层（已完成）
│   │   ├── base.py               # 数据源抽象基类
│   │   ├── exceptions.py         # 异常定义
│   │   ├── dialect.py            # SQL 方言处理
│   │   ├── registry.py           # 数据源注册中心
│   │   └── adapters/             # 数据源适配器
│   │
│   ├── task/                     # 任务模块（复用）
│   ├── executor/                 # 执行器模块（复用）
│   ├── handler/                  # 消息处理器（复用）
│   ├── security/                 # 安全模块（新增）
│   └── util/                     # 工具模块（复用）
│
├── config/                       # 配置文件目录
│   ├── datasources.yaml          # 数据源配置
│   ├── llm.yaml                  # LLM 配置
│   └── prompts/                  # Prompt 模板
│
├── tests/                        # 测试目录
├── docs/                         # 文档目录
├── main.py                       # 主入口
└── requirements.txt              # 依赖列表
```

---

## 九、开发路线图

### 第一阶段：基础设施搭建（2 周）✅

**目标**：建立数据源抽象层和基础框架

**任务清单**：
- ✅ 实现数据源抽象基类和异常体系
- ✅ 实现 Oracle 适配器
- ✅ 实现 PostgreSQL 适配器
- ✅ 实现 Doris 适配器
- ✅ 实现数据源注册中心
- ✅ 创建配置文件示例

**交付物**：
- ✅ 完整的数据源抽象层代码
- ✅ 3 个数据库适配器
- ✅ 配置文件模板
- ✅ 使用文档

### 第二阶段：Agent 核心能力开发（3 周）

**目标**：实现 Agent 核心逻辑和基础工具

**任务清单**：
- [ ] 集成 LangChain 和 LangGraph
- [ ] 实现 LLM Router
- [ ] 实现意图识别模块
- [ ] 实现 Agent 编排器
- [ ] 实现对话管理器
- [ ] 实现 SQL 查询工具
- [ ] 实现数据分析工具
- [ ] 实现 Excel 生成工具

**交付物**：
- Agent 核心代码
- 基础工具集
- LLM 路由器
- 对话管理系统

### 第三阶段：集成与增强（3 周）

**目标**：集成钉钉、实现智能定时任务

**任务清单**：
- [ ] 重构钉钉消息处理器
- [ ] 实现钉钉发送工具
- [ ] 实现定时任务工具
- [ ] 增强 Agent 编排器
- [ ] 实现 SQL 安全验证
- [ ] 实现审计日志

**交付物**：
- 集成钉钉的完整 Agent 系统
- 智能定时任务功能
- 安全模块
- 审计日志系统

### 第四阶段：测试、优化与部署（2 周）

**目标**：完善测试、优化性能、准备部署

**任务清单**：
- [ ] 编写集成测试
- [ ] 性能测试和优化
- [ ] 编写 Docker 配置
- [ ] 编写部署文档
- [ ] 编写用户手册

**交付物**：
- 完整的测试套件
- Docker 镜像
- 部署文档
- 用户手册

---

## 十、风险评估

### 10.1 技术风险

| 风险 | 影响 | 应对策略 |
|------|------|---------|
| LLM 稳定性 | 高 | 多模型降级策略、本地模型备用 |
| SQL 生成准确性 | 高 | Few-shot Prompting、SQL 安全验证 |
| 性能瓶颈 | 中 | 查询超时控制、缓存层、连接池优化 |
| 并发问题 | 中 | 异步架构、请求队列、并发限制 |

### 10.2 兼容性风险

| 问题 | 解决方案 |
|------|---------|
| Oracle 连接重构 | 保留原有接口，新增 Adapter 层 |
| Task 系统集成 | Task 基类保持不变，新增 Agent 调用入口 |
| 配置文件格式 | 保持现有 config.yaml，新增 datasources.yaml |

### 10.3 安全风险

| 风险 | 应对措施 |
|------|---------|
| SQL 注入 | 强制参数化查询、SQL 安全验证层 |
| 敏感数据泄露 | 数据脱敏规则、权限控制、审计日志 |
| API 密钥泄露 | 环境变量、密钥加密存储、定期轮换 |

---

## 十一、下一步行动

### 11.1 立即行动

1. ✅ 安装依赖：`pip install oracledb asyncpg mysql-connector-python pydantic sqlparse`
2. ✅ 测试数据源连接
3. 📝 申请 LLM API 密钥（通义千问、DeepSeek）
4. 📚 学习 LangChain 官方文档

### 11.2 第一周行动

1. 搭建 Agent 项目结构
2. 集成 LangChain
3. 实现 LLM Router
4. 编写第一个 Agent 工具

### 11.3 持续沟通

- 每个阶段完成后进行代码审查和优化
- 遇到技术决策点时提供选项和推荐
- 定期同步进度，及时调整计划

---

## 附录

### A. 环境变量配置

```bash
# .env 文件
ORACLE_PASSWORD=your_oracle_password
POSTGRES_PASSWORD=your_postgres_password
DORIS_PASSWORD=your_doris_password

# LLM API Keys
DASHSCOPE_API_KEY=your_qwen_api_key
DEEPSEEK_API_KEY=your_deepseek_api_key
```

### B. 快速测试命令

```bash
# 测试数据源连接
python tests/test_datasource.py

# 运行单元测试
pytest tests/

# 启动服务
python main.py
```

### C. 参考资源

- [LangChain 官方文档](https://python.langchain.com/)
- [LangGraph 文档](https://langchain-ai.github.io/langgraph/)
- [通义千问 API 文档](https://help.aliyun.com/zh/dashscope/)
- [DeepSeek API 文档](https://platform.deepseek.com/docs)

---

**文档版本历史**：
- v1.0 (2026-04-11): 初始版本，完成数据源抽象层设计
