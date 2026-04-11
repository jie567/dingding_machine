import datetime
import pandas as pd
from src.task.Task import Task, task_config
from src.util.excel_writer import excel_copy, batch_excel_writer
from src.util.util_function import normalize_eline, normalize_route


@task_config(
    name="团散价差任务",
    task_type="file",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75",
    excel_path='./resources/excel_template/团散价差数据.xlsx'
)
class TeamPassengerTask(Task):
    def __init__(self):
        super().__init__('团散价差任务')
    def eline_area(self, conn):
        sql_str = f"""
            SELECT 
            DISTINCT SJDC_ELINE,
            UP_LOCATION||DIS_LOCATION segment,
            AREA_REV 
            FROM air1.dj_his_data_big_temp_now 
            order by SJDC_ELINE
        """
        try:
            area_df = conn.query_as_df(sql_str)

            # 标准化 SJDC_ELINE 为排序去重后的字符串，用于匹配
            area_df['normalized_ELINE'] = area_df['SJDC_ELINE'].apply(normalize_eline)
            area_df['normalized_SEGMENT'] = area_df['SEGMENT'].apply(normalize_eline)
            print(f"本次查询到air1.dj_his_data_big_temp_now {len(area_df)}条数据")
            return area_df
        except Exception as e:
            raise  Exception("Error:", e)

    def eline_passenger_price(self, conn, start_date, end_date):
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        sql_str = f"""
        select
    		TK_NUM,
    		BOOKING_STATUS,
    		UP_LOCATION||DIS_LOCATION SEGMENT,
    		UP_LOCATION||'-'||DIS_LOCATION SEGMENT_KEY,
    		substr(FLIGHT_NO,3) FLIGHT_NO,
    		dep_date,
    		SEG_FARE
        from  airext.PUSH_PAX_DETAIL
        where air_code = 'EU'  
        and to_char(dep_date,'YYYY-MM-DD') BETWEEN '{start_date}' AND '{end_date}' 
        and (TOUR_CODE <> 'GROUP' OR TOUR_CODE IS NULL) 
        and TK_NUM is not null and BOOKING_STATUS in ('RR','HK','KK','HL','HN','UN')
        order by UP_LOCATION||DIS_LOCATION
        """
        try:
            detail_df = conn.query_as_df(sql_str)
            detail_df['normalized_segment'] = detail_df['SEGMENT'].apply(normalize_eline)
            print(f"本次查询时间从{start_date}到{end_date}, 查询到airext.PUSH_PAX_DETAIL {len(detail_df)}条数据")
            return detail_df
        except Exception as e:
            raise Exception("Error:", e)

    def eline_detail_plf(self, conn, start_date, end_date):
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        sql_str = f"""
            select
            ELINE,
            UP_LOCATION||DIS_LOCATION segment,
            UP_LOCATION||'-'||DIS_LOCATION segment_key,
            FLIGHT_NO,
            BKD * DIST RPK,
            DISCAP * DIST ASK
            from AIR1.TB_FLIGHT_DETAIL
            where air_code = 'EU'  
            and dep_date BETWEEN '{start_date}' AND '{end_date}'
            order by ELINE,UP_LOCATION||DIS_LOCATION
            """
        try:
            detail_df = conn.query_as_df(sql_str)
            detail_df['normalized_ELINE'] = detail_df['ELINE'].apply(normalize_eline)
            detail_df['normalized_segment'] = detail_df['SEGMENT'].apply(normalize_eline)
            print(f"本次查询时间从{start_date}到{end_date}, 查询到AIR1.TB_FLIGHT_DETAIL {len(detail_df)}条数据")

            return detail_df
        except Exception as e:
            raise Exception("Error:", e)

    def execute_task(self, conn, start_date=None, end_date=None):
        if start_date is None:
            yesterday = datetime.datetime.today().date() - datetime.timedelta(1)
            last_friday = yesterday - datetime.timedelta(6)
        else:
            yesterday = pd.to_datetime(start_date)
            last_friday = pd.to_datetime(end_date)
        task_config = getattr(self.__class__, '_task_config', {})
        excel_path = task_config.get('excel_path', '../../resources/excel_template/团散价差数据.xlsx')
        excel_df = pd.read_excel(excel_path, sheet_name='Sheet1', header=1)
        area_df = self.eline_area(conn)
        area_df.dropna(subset='AREA_REV', inplace=True)
        eline_to_area = dict(zip(area_df['normalized_ELINE'], area_df['AREA_REV']))
        segment_to_area = dict(zip(area_df['normalized_SEGMENT'], area_df['AREA_REV']))

        detail_price_df = self.eline_passenger_price(conn, last_friday, yesterday)
        segment_price_df = detail_price_df.groupby('SEGMENT_KEY').agg(
            ELINE_FARE=('SEG_FARE', 'sum'),
            BKD=('SEG_FARE', 'count')
        ).reset_index()
        segment_price_df['avg_price'] = segment_price_df['ELINE_FARE'] / segment_price_df['BKD']
        segment_to_avg_price = dict(zip(segment_price_df['SEGMENT_KEY'], segment_price_df['avg_price']))

        detail_plf_df = self.eline_detail_plf(conn, last_friday, yesterday)
        segment_plf_df = detail_plf_df.groupby('SEGMENT_KEY').agg(
            RPK=('RPK', 'sum'),
            ASK=('ASK', 'sum')
        ).reset_index()
        segment_plf_df['PLF'] = segment_plf_df['RPK'] / segment_plf_df['ASK']
        segment_to_plf = dict(zip(segment_plf_df['SEGMENT_KEY'], segment_plf_df['PLF']))

        for index, row in excel_df.iterrows():
            route = str(row['航线']).strip()
            if not route or route == 'nan':
                continue

            areas_found = set()
            temp_avg_price = 0
            temp_segment_plf = []

            if '//' in route:
                sub_routes = [s.strip() for s in route.split('//') if s.strip()]
            elif route.count('-') >= 2:
                parts = route.split('-')
                sub_routes = [f"{parts[i]}-{parts[i + 1]}" for i in range(len(parts) - 1)]
            else:
                sub_routes = [route]

            for sub_route in sub_routes:
                norm_route = normalize_route(sub_route)
                if norm_route in eline_to_area:
                    area_value = eline_to_area[norm_route]
                    areas_found.add(area_value)

                elif norm_route in segment_to_area:
                    area_value = segment_to_area[norm_route]
                    areas_found.add(area_value)
                else:
                    print(f'【ERROR】收益大区未匹配的航线：{norm_route}')
                    areas_found.add("新疆")

                if sub_route in segment_to_avg_price:
                    temp_avg_price += segment_to_avg_price[sub_route]

                if sub_route in segment_to_plf:
                    temp_segment_plf.append(segment_to_plf[sub_route])

            excel_df.at[index, '收益大区'] = ', '.join(areas_found)
            excel_df.loc[index, '散客往返价格'] = round(temp_avg_price, 2)
            if len(temp_segment_plf) != 0:
                excel_df.loc[index, '往返平均客座率'] = sum(temp_segment_plf) / len(temp_segment_plf)
            else:
                excel_df.loc[index, '往返平均客座率'] = 0
            areas_found.clear()

        new_excel_path = excel_copy(excel_path)
        results = []
        results.append((excel_df, 'Sheet1'))
        batch_excel_writer(new_excel_path, results)
        self.file_list.append(new_excel_path)


if __name__ == '__main__':
    # oracle_conn = OracleDataConn()
    t_task = TeamPassengerTask()
    # t_task.execute(oracle_conn)
