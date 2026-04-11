import datetime
import re

import numpy as np
import pandas as pd

from src.task.Task import Task, task_config
from src.util.excel_writer import excel_copy, batch_excel_writer
from src.util.oracle_connect import OracleDataConn


@task_config(
    name="收益任务月底目标校验",
    task_type="file",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75",
    excel_path='../../resources/excel_template/航线月底任务校验.xlsx',
    input_path='../../resources/input_excel/航线区域表.xlsx',
    sql_file2='../../resources/sql/月底任务分解.sql',
    sql_file3='../../resources/sql/月度任务分解区域座收.sql'
)
class MonthDecomposeTask(Task):
    def __init__(self):
        super().__init__('收益任务月底目标校验')

    def date_deter(self, cur_date):
        today = datetime.date.today()  if cur_date is None else pd.to_datetime(cur_date)
        next_month = 1 if today.month == 12 else today.month + 1
        next_year = today.year + 1 if today.month == 12 else today.year
        huanqi_month = today.month - 1 if today.month > 1 else 12
        huanqi_year = today.year if today.month > 1 else today.year - 1

        cur_month = datetime.date(today.year, today.month, 1).strftime('%Y-%m-%d')
        cur_month_end = (datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        T_month_1 = datetime.date(huanqi_year, huanqi_month, 1).strftime('%Y-%m-%d')
        T_month_1_end = (datetime.date(today.year, today.month, 1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        return cur_month, cur_month_end,  T_month_1, T_month_1_end
    def compute_total_discount(self, input_df: pd.DataFrame, time_discount_map: dict) -> pd.DataFrame:
        df = input_df.copy()
        df['discount_rate'] = df['UPDIS_TIME'].map(time_discount_map)
        if df['discount_rate'].isnull().any():  # 检查是否有时间在时刻系数表中找不到（理论上不应发生，但为了健壮性）
            missing = df.loc[df['discount_rate'].isnull(), 'UPDIS_TIME'].unique()
            print(f"警告：以下时间在时刻系数表中未找到，将用 0.42 代替: {missing}")
            df['discount_rate'].fillna(0.42, inplace=True)

        # 计算ELINE  (AB折扣率 * 航班数+BC折扣率 * 航班数)/(AB航班数+BC航班数) 按 ELINE 汇总
        def weighted_mean(group):
            numerator = (group['FLIGHT_NUM'] * group['discount_rate']).sum()
            denominator = group['FLIGHT_NUM'].sum()
            return numerator / denominator if denominator != 0 else 0

        result = df.groupby(['ELINE', 'FLIGHT_NO', 'PLANE'], as_index=False).apply(weighted_mean)
        result.columns = ['ELINE', 'FLIGHT_NO', 'PLANE', 'total_discount']  # 重命名结果列
        return result

    def cal_time_df(self, time_discount_map, cur_time_df, huanqi_time_df):
        db_df3_discount = self.compute_total_discount(cur_time_df, time_discount_map)
        db_df4_discount = self.compute_total_discount(huanqi_time_df, time_discount_map)
        time_df = pd.merge(db_df3_discount, db_df4_discount,
                           on=['ELINE', 'FLIGHT_NO', 'PLANE'], how='left',suffixes=('_cur', '_huanqi'))
        time_df['time_factor'] = (time_df['total_discount_cur'] / time_df['total_discount_huanqi']).round(2)
        # time_df['time_factor'] = (1+(time_df['total_discount_cur']-time_df['total_discount_huanqi']) / time_df['total_discount_huanqi']).round(2)
        time_df.drop(['total_discount_cur', 'total_discount_huanqi'], axis=1, inplace=True)
        return time_df


    def is_English(self, text):
        """判断字符串是否不包含英文字符（返回 True 表示有英文）"""
        if pd.isna(text):
            return False
        return bool(re.search(r'[a-zA-Z]', str(text)))

    def split_segments(self, value):
        """按'/'拆分字符串，返回非空列表"""
        if pd.isna(value) or value == '':
            return []
        return [seg.strip() for seg in str(value).split('/') if seg.strip()]


    def match_sy(self, segment, sy_df, date):
        """
        在 sy 表中匹配一个子段，返回 (SUM_INCOME, SUM_ASK) 的和
        segment 可能是纯英文（如 "AKU-AAT"）或中英混合（如 "新疆-XUH"）
        """
        if pd.isna(segment):
            return 0.0, 0.0
        parts = segment.split('-', 1)
        if len(parts) != 2:
            return 0.0, 0.0
        chn_part = parts[0].strip()
        eng_part = parts[1].strip()

        if self.is_English(segment) and not self.is_English(chn_part):
            # 中英混合：提取中文省份和英文地点列表 """ is_English 判断字符串是否不包含英文字符（返回 True 表示有英文）"""
            # 过滤 sy_df 满足条件
            mask = (sy_df['UP_PROVINCE'] == chn_part) & \
                   (sy_df['DIS_LOCATION'] == eng_part) & \
                   (sy_df['DEP_DATE'] == date)
            matched = sy_df.loc[mask]

        elif self.is_English(segment) and not self.is_English(eng_part):
            mask = (sy_df['UP_LOCATION'] == chn_part) & \
                   (sy_df['DIS_PROVINCE'] == eng_part) & \
                   (sy_df['DEP_DATE'] == date)
            matched = sy_df.loc[mask]
        else:
            # 全英文：直接匹配 SEGMENT
            mask = (sy_df['SEGMENT'] == segment) & \
                   (sy_df['DEP_DATE'] == date)
            matched = sy_df.loc[mask]

        if matched.empty:
            return 0.0, 0.0
        return matched['SUM_INCOME'].sum(), matched['SUM_ASK'].sum()

    def match_eu(self, segment, eu_df, date):
        """
        在 eu 表中匹配一个子段（纯中文），返回 (SUM_INCOME, SUM_ASK) 的和
        """
        if pd.isna(segment):
            return 0.0, 0.0
        mask = (eu_df['TAG'] == segment) & \
               (eu_df['DEP_DATE'] == date)
        matched = eu_df.loc[mask]
        if matched.empty:
            return 0.0, 0.0
        return matched['SUM_INCOME'].sum(), matched['SUM_ASK'].sum()

    def process_region(self, region_value, eu_cur_df, sy_cur_df, date):
        """
        处理一个区域字段（可能包含 '/' 分割的多个子段） # 全中文用eu，其他用sy
        返回 (tongqi_income, tongqi_ask, huanqi_income, huanqi_ask)
        """
        segments = self.split_segments(region_value)
        cur_income, cur_ask = 0.0, 0.0

        for seg in segments:
            if not self.is_English(seg):
                # 纯中文 -> 使用 eu 表  且可能含 '/'，但这里 seg 已经是单个子段，直接匹配
                inc, ask = self.match_eu(seg, eu_cur_df, date)
                cur_income += inc
                cur_ask += ask
            else:
                # 英文或混合 -> 使用 sy 表
                inc, ask = self.match_sy(seg, sy_cur_df, date)
                cur_income += inc
                cur_ask += ask

        return cur_income, cur_ask,


    def add_adjustment_columns(self, cur_detail_df, eu_cur_df, sy_cur_df):
        cur_detail_df['CUR_INCOME_1'] = 0.0
        cur_detail_df['CUR_ASK_1'] = 0.0

        cur_detail_df['CUR_INCOME_2'] = 0.0
        cur_detail_df['CUR_ASK_2'] = 0.0

        for idx, row in cur_detail_df.iterrows():
            DEP_DATE = row['DEP_DATE']
            region1 = row.get('区域1', None)
            region2 = row.get('区域2', None)
            if not pd.isna(region1):
                c_inc, c_ask = self.process_region(region1, eu_cur_df, sy_cur_df, DEP_DATE)
                cur_detail_df.at[idx, 'CUR_INCOME_1'] += c_inc
                cur_detail_df.at[idx, 'CUR_ASK_1'] += c_ask
            if not pd.isna(region2):
                c_inc, c_ask = self.process_region(region2, eu_cur_df, sy_cur_df, DEP_DATE)
                cur_detail_df.at[idx, 'CUR_INCOME_2'] += c_inc
                cur_detail_df.at[idx, 'CUR_ASK_2'] += c_ask

        cur_detail_df['REGION_RASK'] = np.where(
            cur_detail_df['CUR_ASK_2'] <= 0,
            cur_detail_df['CUR_INCOME_1'] / cur_detail_df['CUR_ASK_1'].replace(0, np.nan),
            (cur_detail_df['CUR_INCOME_1'] / cur_detail_df['CUR_ASK_1'].replace(0, np.nan)) * 0.5 +
            (cur_detail_df['CUR_INCOME_2'] / cur_detail_df['CUR_ASK_2'].replace(0, np.nan)) * 0.5
        )
        return cur_detail_df

    def execute_once(self, conn, cur_month, cur_month_end, huanqi_month, huanqi_month_end):
        task_config = getattr(self.__class__, '_task_config', {})

        input_file = task_config.get('input_path', '../../resources/input_excel/航线区域表.xlsx')
        sql_file2 = task_config.get('sql_file2', '../../resources/sql/月底任务分解.sql')
        sql_file3 = task_config.get('sql_file3', '../../resources/sql/月度任务分解区域座收.sql')

        with open(sql_file2, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
        replacements = {
            'huanqi_month_end': huanqi_month_end,
            'huanqi_month': huanqi_month,
            'cur_month_end': cur_month_end,
            'cur_month': cur_month
        }
        df_dict = {}
        for idx, sql_command in enumerate(sql_commands, start=1):
            for placeholder, value in replacements.items():
                pattern = r'\b' + re.escape(placeholder) + r'\b'
                sql_command = re.sub(pattern, value, sql_command)
            df_dict[idx] = conn.query_as_df(sql_command)

        cur_detail_df = df_dict[1][~df_dict[1]['FLIGHT_NO'].astype(str).str.contains('[a-zA-Z]', na=False)]
        execl_area_df = pd.read_excel(input_file, '航线明细', usecols=[1, 2, 3])
        cur_detail_df = pd.merge(cur_detail_df, execl_area_df, how='left', on='ELINE')

        cur_time_df = df_dict[2][~df_dict[2]['FLIGHT_NO'].astype(str).str.contains('[a-zA-Z]', na=False)]
        huanqi_time_df = df_dict[3][~df_dict[3]['FLIGHT_NO'].astype(str).str.contains('[a-zA-Z]', na=False)]
        excel_time_df = pd.read_excel(input_file, sheet_name='时刻系数', dtype={'时间': str})
        excel_time_df['时间'] = excel_time_df['时间'].astype(str).str.rsplit(':', n=1).str[0]
        time_discount_map = dict(zip(excel_time_df['时间'], excel_time_df['折扣']))

        time_df = self.cal_time_df(time_discount_map, cur_time_df, huanqi_time_df)
        print('len(time_df): ', len(time_df), ',\ntime_df.isna().sum(): \n', time_df.isna().sum())

        cur_time_group = cur_time_df.groupby(['ELINE', 'FLIGHT_NO', 'PLANE', 'SEGMENT_TYPE']).agg({
            'FLIGHT_NUM': 'sum',
            'AVG_DIST': 'mean'
        })
        cur_time_group = cur_time_group.groupby(['ELINE', 'FLIGHT_NO', 'PLANE']).agg({
            'FLIGHT_NUM': 'mean',
            'AVG_DIST': 'mean'
        })
        cur_time_df = pd.merge(time_df, cur_time_group, on=['ELINE', 'FLIGHT_NO', 'PLANE'], how='left')

        with open(sql_file3, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        eu_sql, sy_sql = sql_content.split(';')
        eu_cur_sql = eu_sql.replace('start_date', cur_month).replace('end_date', cur_month_end)
        eu_cur_df = conn.query_as_df(eu_cur_sql)

        sy_cur_sql = sy_sql.replace('start_date', cur_month).replace('end_date', cur_month_end)
        sy_cur_df = conn.query_as_df(sy_cur_sql)

        cur_detail_df = self.add_adjustment_columns(cur_detail_df, eu_cur_df, sy_cur_df)
        print('分天明细 cur_detail_df.isna().sum()：\n', cur_detail_df.isna().sum())
        cur_group_df = cur_detail_df.groupby(['ELINE', 'FLIGHT_NO', 'PLANE'], as_index=False).agg({
            'REGION_RASK': 'mean',
            'SUM_INCOME': 'sum',
            'SUM_ASK': 'sum'
        })
        cur_group_df['CUR_RASK'] = cur_group_df['SUM_INCOME'] / cur_group_df['SUM_ASK'].replace(0, np.nan)
        merged_df = pd.merge(cur_group_df, cur_time_df, how='left', on=['ELINE', 'FLIGHT_NO', 'PLANE'])
        print('最终结果len(merged_df): ', len(merged_df), ',\nmerged_df.isna().sum(): \n', merged_df.isna().sum())
        result_df = merged_df[['ELINE', 'FLIGHT_NO', 'PLANE', 'REGION_RASK', 'CUR_RASK', 'time_factor']]
        return cur_detail_df, result_df

    def execute_task(self, conn, cur_month=None, cur_month_end=None, huanqi_month=None, huanqi_month_end=None):
        task_config = getattr(self.__class__, '_task_config', {})
        output_excel_path = task_config.get('excel_path', '../../resources/excel_template/航线月底任务校验.xlsx')
        new_excel_path = excel_copy(output_excel_path, current_date=cur_month[0:7])
        if huanqi_month is None:
            cur_month, cur_month_end, huanqi_month, huanqi_month_end = self.date_deter(cur_month)
        print(" cur_month, cur_month_end: ", cur_month, cur_month_end, '\n',
              "huanqi_month,huanqi_month_end:",huanqi_month,huanqi_month_end, '\n')
        cur_detail_df, result_df = self.execute_once(conn, cur_month, cur_month_end, huanqi_month, huanqi_month_end)

        results = []
        special_sheets = ['T-0月分天明细']
        results.append((cur_detail_df, 'T-0月分天明细'))
        i = 1
        while i <= 3:
            cur_month, cur_month_end, huanqi_month, huanqi_month_end = self.date_deter(huanqi_month)
            print(" cur_month, cur_month_end: ", cur_month, cur_month_end, '\n',
                  "huanqi_month,huanqi_month_end:", huanqi_month, huanqi_month_end, '\n')
            cur_detail_df, merged_df = self.execute_once(conn, cur_month, cur_month_end, huanqi_month, huanqi_month_end)
            result_df = pd.merge(result_df, merged_df, how='left', on= ['ELINE', 'FLIGHT_NO', 'PLANE'], suffixes=(f'_{cur_month[5:7]}', f'_{huanqi_month[5:7]}'))
            results.append((cur_detail_df, f'T-{i}月分天明细'))
            special_sheets.append(f'T-{i}月分天明细')
            i+=1

        results.append((result_df, '校验结果'))
        batch_excel_writer(new_excel_path, results, special_sheets=special_sheets)
        self.file_list.append(new_excel_path)



if __name__ == '__main__':
    m = MonthDecomposeTask()
    oracle_conn = OracleDataConn()
    # m.execute(oracle_conn, cur_month='2025-11-01', cur_month_end='2025-11-30')
    # m.execute(oracle_conn, cur_month='2025-12-01', cur_month_end='2025-12-31')
    # m.execute(oracle_conn, cur_month='2026-01-01', cur_month_end='2026-01-31')
    # m.execute(oracle_conn, cur_month='2026-02-01', cur_month_end='2026-02-28')
    # m.execute(oracle_conn, cur_month='2026-03-01', cur_month_end='2026-03-30')
    m.execute(oracle_conn, cur_month='2026-04-01', cur_month_end='2026-04-09', huanqi_month='2026-03-01', huanqi_month_end='2026-03-31')
    # m.execute_once(oracle_conn, cur_month='2026-04-01', cur_month_end='2026-04-09', huanqi_month='2026-03-01', huanqi_month_end='2026-03-31')
    print(m.msg)