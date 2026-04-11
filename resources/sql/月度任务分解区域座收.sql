-- 全中文用eu，其他用sy
with t1 as(
select
AIR_PORT_CODE,
CITY_NAME,
PROVINCE||AIR_PORT_TYPE as tag,
PROVINCE,
AIR_PORT_TYPE
from air1.AIR_PORT_DY_YJ
where PROVINCE in ('新疆', '黑龙江', '内蒙古')
),

t2 as (
select
up_t1.tag||'-'||dis_t1.tag as tag,
ELINE,
flight_no,
UP_LOCATION||DIS_LOCATION as segment,
dep_date,
'start_date' as dep_date_min,
CASE
WHEN UP_ORD + DIS_ORD = 2 THEN 'AC'
WHEN UP_ORD + DIS_ORD = 3 THEN 'BC'
ELSE 'AB'
END
AS segment_type,
DISCAP*DIST as DISASK,
lst_pro_income
from  AIR1.TB_FLIGHT_DETAIL
left join t1 up_t1  on UP_LOCATION = up_t1.AIR_PORT_CODE
left join t1 dis_t1 on DIS_LOCATION = dis_t1.AIR_PORT_CODE
WHERE dep_date BETWEEN 'start_date' AND 'end_date'
AND air_code = 'EU'
AND FLTTYPE <> 'I'
AND up_location in (select AIR_PORT_CODE from t1) and dis_location in (select AIR_PORT_CODE from t1)
)

select
TAG,
DEP_DATE,
TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')  as date_diff,
sum(lst_pro_income) as SUM_INCOME,
sum(DISASK) as SUM_ASK
from t2
group by TAG,DEP_DATE,TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')
order by TAG,DEP_DATE
;

-- SY 计算的区域座收明细
WITH t1 AS (
    SELECT
        AIR_PORT_CODE,
        CITY_NAME,
        PROVINCE
    FROM air1.AIR_PORT_DY
    WHERE PROVINCE IS NOT NULL
),

t2 as (
SELECT
    ELINE,
    flight_no,
	UP_LOCATION,
	DIS_LOCATION,
    up_t1.PROVINCE AS UP_PROVINCE,    -- 出发机场省份
    dis_t1.PROVINCE AS DIS_PROVINCE,  -- 到达机场省份
    dep_date,
    'start_date' AS dep_date_min,
    DIST,
	CAP,
    TY_INCOME
FROM AIREXT.TB_DEPT_SY_DETAIL
LEFT JOIN t1 up_t1 ON UP_LOCATION = up_t1.AIR_PORT_CODE
LEFT JOIN t1 dis_t1 ON DIS_LOCATION = dis_t1.AIR_PORT_CODE
WHERE dep_date BETWEEN 'start_date' AND 'end_date'
AND EX_TIME = '00'
AND FLT_TYPE <> 'I'
)


select
UP_LOCATION||'-'||DIS_LOCATION as segment,
UP_LOCATION,
DIS_LOCATION,
UP_PROVINCE,
DIS_PROVINCE,
DEP_DATE,
TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')  as date_diff,
sum(TY_INCOME)  as SUM_INCOME,
sum(CAP * DIST) as SUM_ASK
from t2
group by UP_LOCATION,DIS_LOCATION,UP_PROVINCE,DIS_PROVINCE,dep_date,TO_DATE(DEP_DATE, 'YYYY-MM-DD') - TO_DATE(DEP_DATE_MIN, 'YYYY-MM-DD')
ORDER BY UP_LOCATION,DIS_LOCATION,dep_date