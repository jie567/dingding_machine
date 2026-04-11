---
name: "aviation-data-analysis"
description: "Provides aviation business data analysis capabilities including core metrics, SQL templates, and task creation. Invoke when creating aviation analysis tasks, optimizing queries, or handling airline revenue data."
---

# Aviation Data Analysis Skill

This skill provides comprehensive aviation business data analysis capabilities for the dingding_machine project, including core metrics definitions, SQL query templates, data processing best practices, and task creation guidance.

## Core Aviation Metrics

### Revenue Metrics
- **座收 (RASK - Revenue per Available Seat Kilometer)**: 总收入 / 可用座公里
  - 含税座收: 含税总收入 / 可用座公里
  - 不含税座收: 不含税总收入 / 可用座公里
- **客收 (Yield)**: 总收入 / 旅客公里 (RPK)
- **小时收入**: 总收入 / 实飞小时
- **运输收入**: 客票收入 + 燃油收入 + 航补收入 + 货邮收入 + 逾重收入 + 退票收入

### Operational Metrics
- **客座率 (PLF - Passenger Load Factor)**: 旅客公里 / 可用座公里 (RPK / ASK)
- **班次**: 航班执行数量
- **编排小时**: 计划飞行时间
- **实飞小时**: 实际飞行时间
- **分摊座公里 (ASK)**: 座位数 × 航距
- **客公里 (RPK)**: 旅客数 × 航距

### Cost Metrics
- **变动成本**: 航油消耗 + 起降费 + 服务费 + 餐食 + 民航发展基金等
- **边贡率**: (总收入 - 变动成本) / 总收入
- **座公里成本 (CASK)**: 总成本 / 可用座公里

### Profit Metrics
- **税前利润**: 不含税总收入 - 总成本
- **税后利润**: 含税总收入 - 总成本 - 税金
- **航线补贴**: 疆内支线补贴 + 新疆保底补贴 + 支线补贴

## Common SQL Query Patterns

### 1. Daily Revenue Report Query
```sql
SELECT
    eline as 航线,
    flight_no as 航班号,
    dep_date as 日期,
    SUM(lst_pro_income) as 总收入,
    SUM(rpk) / NULLIF(SUM(ask), 0) as 客座率,
    SUM(lst_pro_income) / NULLIF(SUM(ask), 0) as 座收,
    SUM(at_time) as 实飞小时
FROM air1.tb_flight_detail
WHERE dep_date = :query_date
    AND air_code = 'EU'
GROUP BY eline, flight_no, dep_date
ORDER BY 总收入 DESC
```

### 2. Year-over-Year Comparison
```sql
WITH current_period AS (
    SELECT eline, SUM(lst_pro_income) as revenue
    FROM air1.tb_flight_detail
    WHERE dep_date BETWEEN :start_date AND :end_date
    GROUP BY eline
),
last_year_period AS (
    SELECT eline, SUM(lst_pro_income) as revenue
    FROM air1.tb_flight_detail
    WHERE dep_date BETWEEN ADD_MONTHS(:start_date, -12) AND ADD_MONTHS(:end_date, -12)
    GROUP BY eline
)
SELECT
    c.eline as 航线,
    c.revenue as 本期收入,
    l.revenue as 同期收入,
    (c.revenue - l.revenue) as 增减额,
    (c.revenue - l.revenue) / NULLIF(l.revenue, 0) as 同比增长率
FROM current_period c
LEFT JOIN last_year_period l ON c.eline = l.eline
```

### 3. Month-over-Month Comparison
```sql
WITH current_month AS (
    SELECT eline, SUM(lst_pro_income) as revenue
    FROM air1.tb_flight_detail
    WHERE dep_date BETWEEN :start_date AND :end_date
    GROUP BY eline
),
last_month AS (
    SELECT eline, SUM(lst_pro_income) as revenue
    FROM air1.tb_flight_detail
    WHERE dep_date BETWEEN ADD_MONTHS(:start_date, -1) AND ADD_MONTHS(:end_date, -1)
    GROUP BY eline
)
SELECT
    c.eline as 航线,
    c.revenue as 本月收入,
    l.revenue as 上月收入,
    (c.revenue - l.revenue) as 增减额,
    (c.revenue - l.revenue) / NULLIF(l.revenue, 0) as 环比增长率
FROM current_month c
LEFT JOIN last_month l ON c.eline = l.eline
```

### 4. Aircraft Type Analysis
```sql
SELECT
    CASE
        WHEN plane LIKE '9%' THEN 'C909'
        ELSE '空客'
    END as 机型分类,
    COUNT(DISTINCT flight_no) as 航班数,
    SUM(lst_pro_income) as 总收入,
    AVG(SUM(lst_pro_income) / NULLIF(SUM(at_time), 0)) as 平均小时收入,
    SUM(rpk) / NULLIF(SUM(ask), 0) as 平均客座率
FROM air1.tb_flight_detail
WHERE dep_date BETWEEN :start_date AND :end_date
GROUP BY
    CASE
        WHEN plane LIKE '9%' THEN 'C909'
        ELSE '空客'
    END
```

### 5. Route Performance Ranking
```sql
SELECT
    eline as 航线,
    COUNT(*) as 班次,
    SUM(lst_pro_income) as 总收入,
    SUM(lst_pro_income) / NULLIF(SUM(ask), 0) as 座收,
    SUM(rpk) / NULLIF(SUM(ask), 0) as 客座率,
    RANK() OVER (ORDER BY SUM(lst_pro_income) DESC) as 收入排名,
    RANK() OVER (ORDER BY SUM(lst_pro_income) / NULLIF(SUM(ask), 0) DESC) as 座收排名
FROM air1.tb_flight_detail
WHERE dep_date BETWEEN :start_date AND :end_date
GROUP BY eline
ORDER BY 总收入 DESC
```

## Task Creation Guide

### Standard Task Template
```python
from src.task.Task import Task, task_config
from src.util.oracle_connect import OracleDataConn
import pandas as pd

@task_config(
    name="任务名称",
    ex_time="0 16 * * *",  # Cron expression
    task_type="file",  # or "msg" or "card"
    chat_id="chat_id_here",
    excel_path='./resources/excel_template/template.xlsx'
)
class YourTaskName(Task):
    def __init__(self):
        super().__init__('任务名称')

    def execute_task(self, conn, **kwargs):
        # 1. Build SQL query
        sql = self.build_sql(kwargs.get('query_date'))

        # 2. Execute query
        df = conn.query_as_df(sql)

        # 3. Process data
        df = self.process_data(df)

        # 4. Generate output
        if self.task_type == 'file':
            self.generate_excel(df, kwargs.get('excel_path'))
        else:
            self.msg = self.format_message(df)

    def build_sql(self, query_date):
        return f"""
        SELECT * FROM your_table
        WHERE dep_date = '{query_date}'
        """

    def process_data(self, df):
        # Data processing logic
        return df

    def generate_excel(self, df, excel_path):
        # Excel generation logic
        pass
```

### Task Configuration Parameters
- **name**: Task identifier (unique)
- **ex_time**: Cron expression for scheduling
  - Format: `minute hour day month weekday`
  - Example: `0 16 * * *` (every day at 16:00)
  - Example: `30 7,13,15 * * *` (7:30, 13:30, 15:30 daily)
- **task_type**: Output type
  - `file`: Generate Excel file and send to DingTalk
  - `msg`: Send text message to DingTalk
  - `card`: Send interactive card message
- **chat_id**: DingTalk group chat ID
- **excel_path**: Path to Excel template (for file tasks)

## Data Processing Best Practices

### 1. Date Handling
```python
import datetime
import chinese_calendar as calendar

# Get previous workday
def get_previous_workday(date):
    yesterday = date - datetime.timedelta(days=1)
    while not calendar.is_workday(yesterday):
        yesterday -= datetime.timedelta(days=1)
    return yesterday

# Format date string
date_str = datetime.datetime.now().strftime('%Y-%m-%d')
```

### 2. DataFrame Operations
```python
# Safe division with NULL handling
df['metric'] = df['numerator'] / df['denominator'].replace(0, None)

# Percentage formatting
df['rate'] = df['rate'].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else x)

# Conditional aggregation
grouped = df.groupby('column')
result = grouped.agg({
    'revenue': 'sum',
    'flights': 'count',
    'load_factor': 'mean'
})
```

### 3. Excel Generation
```python
from src.util.excel_writer import batch_excel_writer
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font

# Write data to Excel
batch_excel_writer(
    excel_path=template_path,
    sheet_data=[
        {'sheet_name': 'Sheet1', 'df': df1, 'start_row': 2},
        {'sheet_name': 'Sheet2', 'df': df2, 'start_row': 2}
    ]
)

# Apply formatting
wb = load_workbook(output_path)
ws = wb['Sheet1']
for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
    for cell in row:
        cell.alignment = Alignment(horizontal='center', vertical='center')
wb.save(output_path)
```

### 4. Error Handling
```python
def execute_task(self, conn, **kwargs):
    try:
        # Task logic
        df = conn.query_as_df(sql)

        if df.empty:
            self.error = f"No data found for date: {query_date}"
            raise Exception(self.error)

        # Process data

    except Exception as e:
        self.error = f"Task execution failed: {str(e)}"
        raise
```

## Business Rules

### Route Classification
- **直飞**: UP_LOCATION + DIS_LOCATION = ELINE
- **经停**: UP_LOCATION + DIS_LOCATION ≠ ELINE
- **航线性质**: 根据航线属性字段判断（干线、支线、疆内等）

### Aircraft Types
- **C909**: 国产支线飞机（plane LIKE '9%'）
- **空客**: A320系列飞机

### Regional Rules
- **西区**: 不含拉萨航线
- **疆内**: 新疆区域内航线
- **支线**: 支线航空补贴航线

### Special Calculations
- **2867/2868航班**: 特殊变动成本 215210
- **CPA航线**: 需要考虑南航分成和座位还原
- **补贴计算**: 疆内支线补贴 + 新疆保底补贴 + 支线补贴

## Common Use Cases

### 1. Create a Daily Revenue Report
```
User: "使用航空数据分析 Skill，创建一个每日航线收入分析任务"
```
Response will include:
- Task class template with proper decorators
- SQL query for daily revenue data
- Data processing logic
- Excel generation code

### 2. Optimize SQL Query
```
User: "使用航空数据分析 Skill，优化这个查询的性能：[SQL query]"
```
Response will include:
- Performance analysis
- Optimization suggestions
- Refactored SQL query
- Index recommendations

### 3. Calculate Business Metrics
```
User: "使用航空数据分析 Skill，计算上个月所有航线的座收和客座率"
```
Response will include:
- SQL query with proper aggregations
- Metric calculation formulas
- Data validation checks
- Result formatting

### 4. Generate Comparative Analysis
```
User: "使用航空数据分析 Skill，生成航线月度同比环比分析报表"
```
Response will include:
- Year-over-year comparison SQL
- Month-over-month comparison SQL
- Combined analysis template
- Excel output formatting

## Database Tables Reference

### Key Tables
- **air1.tb_flight_detail**: 航班明细数据
- **air1.Tb_market_finance_detail**: 市场财务明细数据
- **air1.TB_DEPT_SY_DETAIL**: 收益明细数据
- **airext.TB_EU_CZ_CPA_FLIGHT**: CPA航班对照表
- **air1.DJ_HIS_DATA_BIG_TEMP_NOW**: 历史数据大表

### Common Fields
- **eline**: 航线（如：CTU-PEK）
- **flight_no**: 航班号
- **dep_date**: 起飞日期
- **plane**: 机型
- **lst_pro_income**: 税后收入
- **rpk**: 旅客公里
- **ask**: 可用座公里
- **at_time**: 实飞小时
- **air_code**: 航空公司代码（EU = 成都航空）

## Integration with DingTalk

### File Sending
```python
from src.util.DingDing_machine import DingDingMachine

machine = DingDingMachine()
machine.set_chat_id(chat_id)
machine.send_file(excel_path)
```

### Message Sending
```python
machine.send_msg("数据分析任务完成")
```

## Notes

1. Always use parameterized queries or proper date formatting to avoid SQL injection
2. Handle NULL values properly in calculations using NULLIF or COALESCE
3. Validate data before processing to avoid empty DataFrame errors
4. Use chinese_calendar for workday calculations
5. Follow the existing task naming conventions
6. Test SQL queries in Oracle before integrating into tasks
7. Document any business-specific logic in comments
8. Use appropriate data types for financial calculations (decimal for currency)
