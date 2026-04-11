from src.task.Task import Task, task_config
from src.util.oracle_connect import OracleDataConn
import datetime
import math

import chinese_calendar as calendar
from src.util.sql_import import normal_monitor_sql, PUSH_PAX_DETAIL_monitor_sql


@task_config(
    name="每日数据库监控任务",
    ex_time="30 7,13,15 * * *",
    task_type="msg",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75"
)
class MonitorExtractTask(Task):
    def __init__(self):
        super().__init__('每日数据库监控任务')

    def monitor_market(self, conn, query_date):
        sql = f'''
        select
        count(DIST)
        from air1.Tb_market_finance_detail
        where dep_date = '{query_date}'
        '''
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            db_df = conn.query_as_df(sql)
            if db_df.iloc[0,0] == 0:
                self.msg += f"\n[{current_time}] monitor_market任务，表：air1.Tb_market_finance_detail 即【市场】没有入库【{query_date}】这天的数据"
            else:
                self.msg += f"\n[{current_time}] monitor_market任务，表：air1.Tb_market_finance_detail 即【市场】已经入库【{query_date}】这天的数据"
        except Exception as e:
            raise Exception(f"[{current_time}] monitor_market任务 表：【air1.TB_DEPT_SY_DETAIL】 即【市场】数据报错: {e}")

    def monitor_extract_rate(self, table, conn):
        sql = normal_monitor_sql(table)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            df = conn.query_as_df(sql)
            tag = None
            if 'SY' in table:
                tag = 'SY'
            elif 'ML' in table:
                tag = 'caiji33'
            else:
                tag = '整合'

            self.msg += f"\n[{current_time}]：表 {table}，即【{tag}】数据已入库条数：{df['SYSDATE_SUM_ROWS'].values[0]}，" \
                        f"过去七天平均条数：{math.ceil(df['LAST7DAY_AVG_ROWS'].values[0])}，是过去7天平均入库条数的 {df['RATE'].values[0] * 100:.2f}%"
        except Exception as e:
            raise Exception(f"[{current_time}] monitor_extract_rate任务 表：【{table}】 即【{tag}】数据报错: {e}")
    def push_extract_rate(self, table, conn):
        sql = PUSH_PAX_DETAIL_monitor_sql(table)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            df = conn.query_as_df(sql)
            self.msg += f"\n[{current_time}]：表 {table}，即【高频】数据已入库条数：{df['SYSDATE_SUM_ROWS'].values[0]}，" \
                        f"过去七天平均条数：{math.ceil(df['LAST7DAY_AVG_ROWS'].values[0])}，是过去7天平均入库条数的 {df['RATE'].values[0] * 100:.2f}%"
        except Exception as e:
            raise Exception(f"[{current_time}] monitor_extract_rate任务 表：【{table}】出错: {e}")

    def monitor_flight_at_time(self, conn):
        sql = '''
        select 
        dep_date,
        eline,
        flight_no,
        at_time
        from air1.tb_flight_detail
        where dep_date between '2025-01-01' and to_char(sysdate,'YYYY-MM-DD')
        and (at_time < 0 or at_time > 15)
        '''
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            df = conn.query_as_df(sql)
            if df.empty or df is None or len(df) == 0:
                self.msg += f"\n[{current_time}]：【整合】tb_flight_detail表无at_time异常"
            else:
                self.msg += f"\n[{current_time}]：【整合】tb_flight_detail表出现at_time异常，详情如下：\n"
                self.msg += df.to_string(index=False)
        except Exception as e:
            raise Exception(f"[{current_time}] monitor_flight_at_time 表：【整合】出错: {e}")

    def execute_task(self, conn):
        now = datetime.datetime.now()
        current_hour = now.hour
        if current_hour == 7:
            single_table = 'airext.PUSH_PAX_DETAIL'
            self.push_extract_rate(single_table, conn)
            self.monitor_extract_rate('airext.tb_flight_DETAIL', conn)
            self.monitor_flight_at_time(conn)
        elif current_hour == 13:
            table_name_list = ['airext.TB_FLIGHT_ML_S1', 'airext.TB_FLIGHT_ML_S2']
            for table_name in table_name_list:
                self.monitor_extract_rate(table_name, conn)
        elif current_hour == 15:
            self.monitor_extract_rate('air1.TB_DEPT_SY_DETAIL', conn)
            if calendar.is_workday(now.date()) and not calendar.is_workday(now.date()):
                while (not calendar.is_workday(now.date())):
                    query_date = now.strftime("%Y-%m-%d")
                    self.monitor_market(conn, query_date)
                    now = now - datetime.timedelta(days=1)
                if calendar.is_workday(now.date()):
                    self.monitor_market(conn, query_date)



if __name__ == '__main__':
    oracle_conn = OracleDataConn()
    monitor_task = MonitorExtractTask()
    # single_table = 'airext.PUSH_PAX_DETAIL'
    # monitor_task.push_extract_rate(single_table, oracle_conn)
    # monitor_task.monitor_extract_rate('airext.tb_flight_DETAIL', oracle_conn)
    # monitor_task.monitor_flight_at_time(oracle_conn)

    now = datetime.datetime.now()
    query_date = now.strftime("%Y-%m-%d")
    # monitor_task.monitor_market(oracle_conn, query_date)
    monitor_task.execute_task(oracle_conn)
    print(monitor_task.msg)