-- 环期分天明细计算--------
with t1 as(
SELECT
	eline,
    FLIGHT_NO,
	UP_LOCATION||DIS_LOCATION as segment,
    dep_date,
-- 	min(dep_date) over(partition by ELINE,flight_no order by dep_date) as dep_date_min,
	'huanqi_month' as dep_date_min,
	UP_TIME,
		case
		when UP_LOCATION||DIS_LOCATION = Eline
		then '直飞'
		else '经停' end as flight_type,
	CASE
    WHEN UP_ORD + DIS_ORD = 2 THEN 'AC'
	WHEN UP_ORD + DIS_ORD = 3 THEN 'BC'
    ELSE 'AB'
    END
	AS segment_type,
    CASE
        WHEN PLANE LIKE '9%' THEN 'C909'
        ELSE '空客'
    END AS plane
FROM AIR1.TB_FLIGHT_DETAIL
WHERE dep_date BETWEEN 'huanqi_month' and 'huanqi_month_end'
    AND air_code = 'EU'
    AND FLTTYPE <> 'I'
-- 	AND lst_pro_income <> 0
)

select
ELINE,
FLIGHT_NO,
PLANE,
DEP_DATE,
TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')  as date_diff
-- round(sum(lst_pro_income) / nullif(sum(CAP * DIST),0),3) as RASK
-- sum(lst_pro_income) as SUM_INCOME,
-- sum(CAP * DIST) as SUM_ASK
from t1
group by  eline, flight_no, PLANE, dep_date, TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')
ORDER BY  eline, flight_no, dep_date
;


-- 环期实际座收结果(不分天，月统计) --

with t1 as (
SELECT
	eline,
    FLIGHT_NO,
	UP_LOCATION||DIS_LOCATION as segment,
    dep_date,
	UP_TIME,
	CASE
		when UP_LOCATION||DIS_LOCATION = Eline
		then '直飞'
		else '经停'
	end as flight_type,
	CASE
        WHEN UP_ORD + DIS_ORD = 2 THEN 'AC'
		WHEN UP_ORD + DIS_ORD = 3 THEN 'BC'
    ELSE 'AB'
    END AS segment_type,
    CASE
        WHEN PLANE LIKE '9%' THEN 'C909'
        ELSE '空客'
    END AS plane,
    lst_pro_income,
    BKD,
    DISCAP,
    DIST
FROM AIR1.TB_FLIGHT_DETAIL
-- WHERE dep_date BETWEEN  '2025-11-01' and '2025-11-30'
WHERE dep_date BETWEEN 'huanqi_month' and 'huanqi_month_end'
    AND air_code = 'EU'
    AND FLTTYPE <> 'I'
ORDER BY  eline, flight_no, dep_date,segment_type
)


select
eline,
FLIGHT_no,
PLANE,
round(sum(lst_pro_income)/sum(DISCAP * DIST),4) as HUANQI_ACT_RASK
from  t1
group by eline,FLIGHT_no,PLANE
ORDER BY  eline
;


----- 当期/环期 时刻调整系数计算 ----

with t1 as(
select
eline,
flight_no,
UP_LOCATION||DIS_LOCATION as segment,
CASE
WHEN PLANE LIKE '9%' THEN 'C909'
ELSE '空客'
END AS plane,
dep_date,
DIST,
min(UPDIS_TIME) over(partition by eline,flight_no,dep_date,UP_LOCATION||DIS_LOCATION ) as UPDIS_TIME,
CASE
WHEN UP_ORD + DIS_ORD = 2 THEN 'AC'
WHEN UP_ORD + DIS_ORD = 3 THEN 'BC'
ELSE 'AB'
END
AS segment_type
from AIREXT.TB_INPORT_DETAIL
where dep_date between 'cur_month' and 'cur_month_end'
AND air_code = 'EU'
AND EX_DATE = 'cur_month'
AND EX_TIME = '00:00'
AND FLT_TYPE <> 'I'
)

select
eline,
flight_no,
segment,
plane,
SEGMENT_TYPE,
UPDIS_TIME,
count(*) as flight_num,
avg(DIST) avg_dist
from t1
where segment_type <> 'AC'
group by
eline,
flight_no,
segment,
plane,
SEGMENT_TYPE,
UPDIS_TIME
order by eline,flight_no, segment, UPDIS_TIME
;




with t1 as(
select
eline,
flight_no,
UP_LOCATION||DIS_LOCATION as segment,
CASE
WHEN PLANE LIKE '9%' THEN 'C909'
ELSE '空客'
END AS plane,
dep_date,
DIST,
min(UPDIS_TIME) over(partition by eline,flight_no,dep_date,UP_LOCATION||DIS_LOCATION ) as UPDIS_TIME,
CASE
WHEN UP_ORD + DIS_ORD = 2 THEN 'AC'
WHEN UP_ORD + DIS_ORD = 3 THEN 'BC'
ELSE 'AB'
END
AS segment_type
from AIREXT.TB_INPORT_DETAIL
where dep_date between 'huanqi_month' and 'huanqi_month_end'
AND air_code = 'EU'
AND ex_dif =  -1
AND EX_TIME = '00:00'
AND FLT_TYPE <> 'I'
)

select
eline,
flight_no,
segment,
plane,
SEGMENT_TYPE,
UPDIS_TIME,
count(*) as flight_num,
avg(DIST) avg_dist
from t1
where segment_type <> 'AC'
group by
eline,
flight_no,
segment,
plane,
SEGMENT_TYPE,
UPDIS_TIME
order by eline,flight_no, segment, UPDIS_TIME











