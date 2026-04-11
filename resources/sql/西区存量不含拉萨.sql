with t1 as (
select
dep_date,
last_dep_date,
round((sum(LST_PRO_INCOME) - sum(ADD_INCOME))/sum(ask),3) as INCOME,
round((sum(LST_PRO_INCOME) - sum(ADD_INCOME))/sum(BKD),2) as avg_price
from air1.dj_his_data_big_temp_now
where AREA_REV = '西部'
and add_sto_tb = '存量'
and UP_LOCATION <> 'LXA' and DIS_LOCATION <> 'LXA'
and dep_date between to_char(sysdate-8,'YYYY-MM-DD') and to_char(sysdate-1,'YYYY-MM-DD')
group by dep_date,last_dep_date
),

t2 as(
select
dep_date,
round((sum(LST_PRO_INCOME) - sum(ADD_INCOME))/sum(ask),3) as INCOME_LAST,
round((sum(LST_PRO_INCOME) - sum(ADD_INCOME))/sum(BKD),2) as avg_price
from air1.dj_his_data_big_temp_now
where AREA_REV = '西部'
and add_sto_tb = '存量'
and UP_LOCATION <> 'LXA' and DIS_LOCATION <> 'LXA'
and  dep_date between (select min(last_dep_date) from t1) and (select max(last_dep_date) from t1)
group by dep_date
)

select
t1.*,
t2.*,
t1.avg_price - t2.avg_price as avg_price_TONGBI,
t1.INCOME - t2.INCOME_LAST as INCOME_TONGBI
from t1 left join t2
on t1.last_dep_date =  t2.dep_date
order by  t1.dep_date