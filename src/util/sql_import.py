


def normal_monitor_sql(table_name):
    return f"""
with t1 as (
select 
TO_CHAR(SYSDATE,'YYYY-MM-DD') as compare_date,
count(*)/7 as last7day_avg_rows  
from  {table_name}
where  ex_date <= TO_CHAR(SYSDATE-1,'YYYY-MM-DD') and  ex_date >= TO_CHAR(SYSDATE-7,'YYYY-MM-DD')
),

t2 as (
select 
ex_date, 
count(*) as sysdate_sum_rows  
from  {table_name}
where  ex_date = TO_CHAR(SYSDATE,'YYYY-MM-DD')
group by ex_date
)

select
t2.ex_date, 
t2.sysdate_sum_rows,
t1.last7day_avg_rows,
round(t2.sysdate_sum_rows/t1.last7day_avg_rows,5) as rate
from t1 join t2
on t1.compare_date = t2.ex_date
"""


def PUSH_PAX_DETAIL_monitor_sql(table_name):
    return f"""
with t1 as (
select 
TO_CHAR(SYSDATE,'YYYY-MM-DD') as compare_date,
count(*)/7 as last7day_avg_rows  
from  {table_name}
where  TO_CHAR(ex_date,'YYYY-MM-DD') <= TO_CHAR(SYSDATE-1,'YYYY-MM-DD') 
and  TO_CHAR(ex_date,'YYYY-MM-DD') >= TO_CHAR(SYSDATE-7,'YYYY-MM-DD')
and coupon_status in ('F','C')
),

t2 as (
select 
TO_CHAR(ex_date,'YYYY-MM-DD') as ex_date, 
count(*) as sysdate_sum_rows  
from  {table_name}
where  TO_CHAR(ex_date,'YYYY-MM-DD') = TO_CHAR(SYSDATE,'YYYY-MM-DD') and coupon_status in ('F','C')
group by ex_date
)

select
t2.ex_date, 
t2.sysdate_sum_rows,
t1.last7day_avg_rows,
round(t2.sysdate_sum_rows/t1.last7day_avg_rows,5) as rate
from t1 join t2
on t1.compare_date = t2.ex_date
"""