import datetime

import pandas as pd

from src.task.Task import Task, task_config
from src.util.excel_writer import excel_copy, batch_excel_writer
from src.util.oracle_connect import OracleDataConn

@task_config(
    name="抖音次卡",
    ex_time="30 9 * * 4",
    task_type="file",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75",
    excel_path ='./resources/excel_template/抖音次卡D舱分析.xlsx'
)
class DailyTicketTask(Task):
    def __init__(self):
        super().__init__('抖音次卡')


    def douyin_card_cal(self, start_date, end_date):
        #  君哥加了 distinct 我觉得不加
        sql_str1 = f'''
    select distinct
    to_char(tk_date,'yyyy-mm-dd') tk_DATE,
    to_char(dep_date,'yyyy-mm-dd') DEP_DATE,
    FLIGHT_NO,
    CONCAT(UP_LOCATION,DIS_LOCATION) segment,
    AC_TYPE_SHORT,
    TK_NUM,
    PNAME,
    PNAME_EN,
    SEG_STATE,
    ORASD_CLASS,
    PASSENGER_TYPE,
    SEG_ADDFARE,
    case 
    when REGEXP_SUBSTR(ORASD_CLASS, '\d+$')='199' then 199/2
    else to_number(REGEXP_SUBSTR(ORASD_CLASS, '\d+$')) end seg_price
    from airext.push_pax_detail
    where to_char(tk_date,'yyyy-mm-dd') between '{start_date}' AND  '{end_date}'
    -- and LEFT(tk_date, 10) between '2025-09-22' and '2025-09-24'
    and SD_CLASS='D'
    and substr(ORASD_CLASS,1,4)='YGCK'
    -- and COUPON_STATUS in ('F','C','L')
    and TK_NUM is not null
    order by DEP_DATE,CONCAT(UP_LOCATION,DIS_LOCATION)
        '''
        sql_str2 = f'''
        with t1 as (
    	select
        to_char(dep_date,'yyyy-mm-dd') DEP_DATE,
        CONCAT(UP_LOCATION,DIS_LOCATION) segment,
    	FLIGHT_NO,
    	SEG_FARE,

        case 
        when substr(ORASD_CLASS,1,4)='YGCK' and REGEXP_SUBSTR(ORASD_CLASS, '\d+$')='199' then 199/2
        when substr(ORASD_CLASS,1,4)='YGCK' then to_number(REGEXP_SUBSTR(ORASD_CLASS, '\d+$')) 
    	else SEG_FARE
    	end seg_price

    	from airext.push_pax_detail
        where to_char(tk_date,'yyyy-mm-dd')  between '{start_date}' AND  '{end_date}'
    		and air_code = 'EU' and air_code_mc = 'EU' and (TOUR_CODE <> 'GROUP' OR TOUR_CODE IS NULL) 
    --         and COUPON_STATUS in ('F','C','L')
            and TK_NUM like '811%' and SD_CLASS not in ('D','A','J')
    		and SEG_FARE > 50 
    		and substr(orasd_class,1,4) not in ('YFLC','YGLC','YFWF','YGXZ','YGXS','YAHX','YASL','YARY','YAGC','YADB','YBSS','YBHX','YBRY',
    		'YBTP','YBJM','YBXX','YCXF','YCDM','YCJC','YCXL','YCFZ','YCJD','YCAQ','YCFF','YCCL','YG5E')
    		and substr(orasd_class,1,3) not in ('YGP','YIN')	
    	),
    -- 	团队最低价
    	t2 as(
    	select
        to_char(dep_date,'yyyy-mm-dd') DEP_DATE,
        CONCAT(UP_LOCATION,DIS_LOCATION) segment,
    	FLIGHT_NO,
    	SEG_FARE,

        case 
        when substr(ORASD_CLASS,1,4)='YGCK' and REGEXP_SUBSTR(ORASD_CLASS, '\d+$')='199' then 199/2
        when substr(ORASD_CLASS,1,4)='YGCK' then to_number(REGEXP_SUBSTR(ORASD_CLASS, '\d+$')) 
    	else SEG_FARE
    	end seg_price

    	from airext.push_pax_detail
        where to_char(tk_date,'yyyy-mm-dd')  between '{start_date}' AND  '{end_date}'
    	and air_code = 'EU' and air_code_mc = 'EU' and TOUR_CODE = 'GROUP' 
    --         and COUPON_STATUS in ('F','C','L')
        and TK_NUM like '811%' and SD_CLASS not in ('D','A','J')
    	and SEG_FARE > 50 
    	and substr(orasd_class,1,4) not in ('YFLC','YGLC','YFWF','YGXZ','YGXS','YAHX','YASL','YARY','YAGC','YADB','YBSS','YBHX','YBRY',
    	'YBTP','YBJM','YBXX','YCXF','YCDM','YCJC','YCXL','YCFZ','YCJD','YCAQ','YCFF','YCCL','YG5E')
    	and substr(orasd_class,1,3) not in ('YGP','YIN')	
    	)

    	select
    	t1.segment,
    	t1.FLIGHT_NO,
    	case when  min(t1.SEG_FARE) < min(t1.seg_price) then min(t1.SEG_FARE)
    	ELSE min(t1.seg_price)
    	END t1_minpirce,

    	case when  min(t2.SEG_FARE) < min(t2.seg_price) then min(t2.SEG_FARE)
    	ELSE min(t2.seg_price)
    	END t2_minpirce
    	from t1 left join t2
    	on  t1.segment = t2.segment and t1.FLIGHT_NO = t2.FLIGHT_NO and t1.DEP_DATE = t2.DEP_DATE
    	group by t1.segment,t1.FLIGHT_NO
            '''
        return sql_str1,sql_str2

    def plf_flight(self, start_date, end_date):
        return f'''
    with t1 as(
        select
    		CONCAT(UP_LOCATION,DIS_LOCATION) as segment,
    		dep_date,
    		'EU'||flight_no as flight_no,
    		max(BKD) BKD,
    		max(share_cap) share_cap
    		from AIREXT.TB_INPORT_DETAIL
    		where AIR_CODE = 'EU' 
    		and dep_date between '{start_date}' AND  '{end_date}'
    		and BKD<>0  and  ex_time='00:00'
    		group by CONCAT(UP_LOCATION,DIS_LOCATION),dep_date,flight_no
    )

    select
    segment,
    FLIGHT_NO,
    dep_date,
    round(BKD/NULLIF(share_cap, 0),6) as flight_plf
    from t1 
        '''

    def plf_segment(self, start_date, end_date):
        return f'''
    with t1 as(
        select
    		CONCAT(UP_LOCATION,DIS_LOCATION) as segment,
    		dep_date,
    		'EU'||flight_no as flight_no,
    		max(BKD) BKD,
    		max(share_cap) share_cap
    		from AIREXT.TB_INPORT_DETAIL
    		where AIR_CODE = 'EU' 
    		and dep_date between '{start_date}' AND  '{end_date}'
    		and BKD<>0  and  ex_time='00:00'
    		group by CONCAT(UP_LOCATION,DIS_LOCATION),dep_date,flight_no
    )

    select
    segment,
    flight_no,
    round(avg(BKD/NULLIF(share_cap, 0)),6) as segment_plf
    from t1 
    group by  segment,flight_no
        '''

    def execute_task(self, conn, start_date=None, end_date=None):
        task_config = getattr(self.__class__, '_task_config', {})
        excel_path = task_config.get('excel_path', '../resources/excel_template/抖音次卡D舱分析.xlsx')
        if start_date is None:
            start_date = datetime.datetime.now() - datetime.timedelta(days=7)
            end_date = datetime.datetime.now() - datetime.timedelta(days=1)
        sql_str1, sql_str2 = self.douyin_card_cal(start_date, end_date)
        db_df1 = conn.query_as_df(sql_str1)
        db_df2 = conn.query_as_df(sql_str2)
        db_df = pd.merge(db_df1, db_df2, on=['SEGMENT', 'FLIGHT_NO'], how='left')

        flight_plf = self.plf_flight(start_date, end_date)
        segment_plf = self.plf_segment(start_date, end_date)
        flight_plf = conn.query_as_df(flight_plf)
        segment_plf = conn.query_as_df(segment_plf)
        db_df = pd.merge(db_df, flight_plf, on=['SEGMENT', 'FLIGHT_NO', 'DEP_DATE'], how='left')
        db_df = pd.merge(db_df, segment_plf, on=['SEGMENT', 'FLIGHT_NO'], how='left')
        db_df.sort_values(by=['SEGMENT', 'FLIGHT_NO', 'DEP_DATE'], inplace=True)
        print(f'最后整合完成的数据有{len(db_df)}条')

        new_excel_path = excel_copy(excel_path, current_date=end_date)
        results = []
        results.append((db_df, '上周数据'))
        batch_excel_writer(new_excel_path, results, special_sheets='上周数据')
        self.file_list.append(new_excel_path)

if __name__ == '__main__':
    oracle_conn = OracleDataConn()
    d_task = DailyTicketTask()
    # d_task.execute(oracle_conn)
    # 可指定日期
    d_task.execute(oracle_conn,start_date='2026-02-01',end_date='2026-02-01')