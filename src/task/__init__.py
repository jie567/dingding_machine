
def airline_sql_str(yesterday_str):
    sql_str_airline = f'''
WITH airport_codes AS (
  SELECT air_port_code
  FROM air1.air_port_dy
  WHERE province = '新疆'
  AND air_port_code IS NOT NULL
),

t2 AS (
  SELECT
    distinct JHWF_ELINECN2, 
    HXXZ, 
    JHWF_FLIGHT_NO, 
    PLANE1,
    PLANE2,
    JHWF_ELINE,
    CASE
      WHEN PLANE2 = 'C909' AND HXXZ = '国内航线'
        AND NOT EXISTS ( -- 判断JHWF_ELINE所有航段是否都在新疆
          SELECT 1
          FROM TABLE(
            CAST(
              MULTISET(
                SELECT REGEXP_SUBSTR(t.JHWF_ELINE, '[^=]+', 1, LEVEL)
                FROM DUAL
                CONNECT BY LEVEL <= REGEXP_COUNT(t.JHWF_ELINE, '=') + 1
              ) AS SYS.ODCIVARCHAR2LIST
            )
          ) s
          WHERE s.COLUMN_VALUE NOT IN (SELECT air_port_code FROM airport_codes)
        )
      THEN 1  
      ELSE 0  
    END AS is_xingjiang_flag
  FROM air1.Tb_market_finance_detail t
  WHERE dep_date = '{yesterday_str}' AND JHWF_ELINECN2 IS NOT NULL
),

t3 as (
SELECT
  tmfd.JHWF_ELINECN2, 
  tmfd.PLANE1,        
  tmfd.JHWF_FLIGHT_NO,
  tmfd.PLANE2,         
  
  CASE 
    WHEN t2.HXXZ = '国际航线' THEN sum(tmfd.INCOME_TOTAL)
    WHEN t2.is_xingjiang_flag = 0 THEN sum(tmfd.INCOME_TOTAL)
    ELSE sum(tmfd.INCOME_TOTAL) + sum(38000 * tmfd.FT_TIME) - sum(tmfd.BRANCH_SUBSIDY) - sum(tmfd.XJ_SUBSIDY) - sum(tmfd.HYSR_ATI + tmfd.HYBCSR_ATI + tmfd.KPSR_ATI 
	     + tmfd.FUEL_ATI) * 1.09 - sum(tmfd.SUBSIDY_ATI)
  END AS AfterTAX_TOTAL_INCOME,
  
  CASE 
    WHEN t2.HXXZ = '国际航线' THEN sum(tmfd.INCOME_TOTAL)
    WHEN t2.is_xingjiang_flag = 0
    THEN sum(tmfd.KPSR_ATI*1.09) + sum(tmfd.FUEL_ATI*1.09) + sum(tmfd.SUBSIDY_ATI*1.04) + sum(tmfd.BRANCH_SUBSIDY) + sum(tmfd.XJ_SUBSIDY) 
         + sum(tmfd.HYSR_ATI*1.09) + sum(tmfd.HYBCSR_ATI*1.09) 
		 + sum(tmfd.YZSR_ATI*1.09) + sum(tmfd.TPSR_ATI * 1.06)
    ELSE sum(38000 * tmfd.FT_TIME) + sum(tmfd.TPSR_ATI * 1.06) + sum(tmfd.YZSR_ATI*1.09)
  END AS PreTAX_TOTAL_INCOME,
  
  CASE
    WHEN t2.is_xingjiang_flag = 0
    THEN SUM(tmfd.MOD_INCOME_PTI)/1.09 - SUM(tmfd.LST_PRO_INCOME_PTI)/1.09
    ELSE 38000 * SUM(tmfd.FT_TIME) - SUM(tmfd.BRANCH_SUBSIDY) - SUM(tmfd.HYSR_ATI + tmfd.HYBCSR_ATI + tmfd.KPSR_ATI + tmfd.FUEL_ATI)* 1.09
  END AS airline_subsidy
  
FROM air1.Tb_market_finance_detail tmfd 
JOIN t2 
ON tmfd.JHWF_ELINECN2 = t2.JHWF_ELINECN2
AND tmfd.PLANE1 = t2.PLANE1
AND tmfd.JHWF_FLIGHT_NO = t2.JHWF_FLIGHT_NO
AND tmfd.PLANE2 = t2.PLANE2

WHERE tmfd.dep_date = '{yesterday_str}' AND tmfd.ABNORMAL_FLIGHTNO <> '异常航班'
GROUP BY 
  tmfd.JHWF_ELINECN2,   
  tmfd.PLANE1,          
  tmfd.JHWF_FLIGHT_NO,  
  tmfd.PLANE2,                     
  t2.HXXZ, 
  t2.is_xingjiang_flag  
)


SELECT
  t1.PLANE2,
  t1.JHWF_ELINECN2, 
  t1.PLANE1,
  ROUND(t3.PreTAX_TOTAL_INCOME / NULLIF(SUM(t1.FT_TIME), 0) / 10000, 3) AS 含税小时收入,
  ROUND(t3.AfterTAX_TOTAL_INCOME / NULLIF(SUM(t1.FT_TIME), 0) / 10000, 3) AS 不含税小时收入,
  ROUND(SUM(t1.LST_PRO_INCOME_PTI-t1.FUEL_PTI) / NULLIF(SUM(t1.FT_TIME), 0) / 10000, 3) AS 含税票面小时收入,
  ROUND(t3.PreTAX_TOTAL_INCOME / NULLIF(SUM(t1.DISASK), 0), 3) AS 含税座收,
  ROUND(t3.AfterTAX_TOTAL_INCOME / NULLIF(SUM(t1.DISASK), 0), 3) AS 不含税座收,
  ROUND((t3.AfterTAX_TOTAL_INCOME-sum(t1.VCOST)) / 10000 / NULLIF(SUM(t1.FT_TIME), 0), 3) AS 小时边贡,
  ROUND((t3.AfterTAX_TOTAL_INCOME-sum(t1.VCOST)) / NULLIF(t3.AfterTAX_TOTAL_INCOME, 0), 3) AS 边贡率,
  ROUND(SUM(t1.BKASK) / NULLIF(SUM(t1.DISASK), 0), 3) AS 客座率,
  ROUND(SUM(t1.VCOST) / NULLIF(SUM(t1.FT_TIME), 0) / 10000, 3) AS 小时变动成本,
  ROUND((t3.AfterTAX_TOTAL_INCOME-sum(t1.VCOST)) / 10000, 3) AS 边贡总额,
  ROUND(SUM(t1.FT_TIME), 3) AS 实飞小时,
  ROUND(t3.PreTAX_TOTAL_INCOME / 10000, 3) AS 含税总收入,
  ROUND(t3.AfterTAX_TOTAL_INCOME / 10000, 3) AS 不含税总收入,
  ROUND(SUM(t1.LST_PRO_INCOME_PTI-t1.FUEL_PTI) / 10000, 3) AS 含税票面收入,
  ROUND(SUM(t1.BRANCH_SUBSIDY) / 10000, 3) AS 支线补贴,
  ROUND(t3.airline_subsidy / 10000, 3) AS 不含税航线补贴,
  t1.JHWF_FLIGHT_NO
FROM 
  air1.Tb_market_finance_detail t1 
LEFT JOIN t3
  ON t1.JHWF_ELINECN2 = t3.JHWF_ELINECN2
  AND t1.PLANE1 = t3.PLANE1
  AND t1.JHWF_FLIGHT_NO = t3.JHWF_FLIGHT_NO
  AND t1.PLANE2 = t3.PLANE2
WHERE 
  t1.dep_date = '{yesterday_str}'  and t1.JHWF_ELINECN2 is not null and t1.ABNORMAL_FLIGHTNO <> '异常航班'
GROUP BY 
  t1.PLANE2,
  t1.JHWF_ELINECN2, 
  t1.PLANE1,
  t1.JHWF_FLIGHT_NO,
  t3.PreTAX_TOTAL_INCOME,
  t3.AfterTAX_TOTAL_INCOME,
  t3.airline_subsidy
    '''
    return sql_str_airline


def sql_airline_detail_str(yesterday_str):
    sql_str_airline_detail = f'''
WITH airport_codes AS (
  SELECT air_port_code
  FROM air1.air_port_dy
  WHERE province = '新疆'
  AND air_port_code IS NOT NULL
),

t1 AS (
  SELECT
    JHWF_ELINECN2, 
    HXXZ, 
    JHWF_FLIGHT_NO, 
    PLANE1, 
    SUM(FLT) AS FLT,
    SUM(JH_FT_TIME) AS JH_FT_TIME,
    SUM(FT_TIME) AS FT_TIME,
    SUM(DISASK) AS DISASK,
    SUM(BKASK) AS BKASK,
    SUM(KPSR_ATI) AS KPSR_ATI,
    SUM(FUEL_ATI) AS FUEL_ATI,
    SUM(SUBSIDY_ATI) AS SUBSIDY_ATI,
    SUM(HYSR_ATI) AS HYSR_ATI,
    SUM(HYBCSR_ATI) AS HYBCSR_ATI,
    SUM(YZSR_ATI) AS YZSR_ATI,
    SUM(TPSR_ATI) AS TPSR_ATI,
    SUM(YSSR_ATI) AS YSSR_ATI,
    SUM(INCOME_TOTAL) AS INCOME_TOTAL,
    SUM(VCOST) AS VCOST,
    SUM(FYJE110) AS FYJE110,
    SUM(FYJE5) AS FYJE5,
    SUM(FYJE18) AS FYJE18,
    SUM(FYJE26) AS FYJE26,
    SUM(FYJE20) AS FYJE20,
    SUM(FYJE21) AS FYJE21,
    SUM(FYJE15) AS FYJE15,
    SUM(FYJE9) AS FYJE9,
    SUM(FYJE17) AS FYJE17,
    SUM(FYJE113) AS FYJE113,
    SUM(FYJE127) AS FYJE127,
    SUM(FYJE142) AS FYJE142,
    SUM(FYJE163) AS FYJE163,
    SUM(FYJE162) AS FYJE162,
    SUM(FYJE487) AS FYJE487,
    SUM(KPQZSY) AS KPQZSY,
    SUM(SUBSIDY_ATI) AS SUBSIDY_ATI2,
    SUM(XJ_SUBSIDY) AS XJ_SUBSIDY,
    SUM(BSP_FEE) AS BSP_FEE,
    SUM(JSXT_FEE) AS JSXT_FEE,
    SUM(ZF_FEE) AS ZF_FEE,
    SUM(AGENT_FEE) AS AGENT_FEE,
    SUM(BRANCH_SUBSIDY) AS BRANCH_SUBSIDY,
    SUM(INCOME_TOTAL) AS INCOME_TOTAL2
      FROM air1.Tb_market_finance_detail 
  WHERE dep_date = '{yesterday_str}' AND JHWF_ELINECN2 IS NOT NULL
  GROUP BY JHWF_ELINECN2, HXXZ, JHWF_FLIGHT_NO, PLANE1
),

t2 AS (
  SELECT
    distinct JHWF_ELINECN2, 
    HXXZ, 
    JHWF_FLIGHT_NO, 
    PLANE1,
    PLANE2,
    JHWF_ELINE,
    CASE
      WHEN PLANE2 = 'C909' AND HXXZ = '国内航线'
        AND NOT EXISTS ( -- 判断JHWF_ELINE所有航段是否都在新疆
          SELECT 1
          FROM TABLE(
            CAST(
              MULTISET(
                SELECT REGEXP_SUBSTR(t.JHWF_ELINE, '[^=]+', 1, LEVEL)
                FROM DUAL
                CONNECT BY LEVEL <= REGEXP_COUNT(t.JHWF_ELINE, '=') + 1
              ) AS SYS.ODCIVARCHAR2LIST
            )
          ) s
          WHERE s.COLUMN_VALUE NOT IN (SELECT air_port_code FROM airport_codes)
        )
      THEN 1  
      ELSE 0  
    END AS is_xingjiang_flag
  FROM air1.Tb_market_finance_detail t
  WHERE dep_date = '{yesterday_str}' AND JHWF_ELINECN2 IS NOT NULL
)

SELECT 
    t1.*,
    t1.XJ_SUBSIDY AS SUBSIDY,
    CASE
      WHEN t2.is_xingjiang_flag = 0
      THEN 0
      ELSE 38000 * t1.FT_TIME - t1.BRANCH_SUBSIDY - t1.XJ_SUBSIDY - (t1.HYSR_ATI + t1.HYBCSR_ATI + t1.KPSR_ATI + t1.FUEL_ATI) * 1.09
    END AS XINJIANG_BAODI,
    
    CASE 
      WHEN t2.is_xingjiang_flag = 0
      THEN t1.INCOME_TOTAL
      ELSE t1.INCOME_TOTAL + 38000 * t1.FT_TIME - t1.BRANCH_SUBSIDY - t1.XJ_SUBSIDY - (t1.HYSR_ATI + t1.HYBCSR_ATI + t1.KPSR_ATI + t1.FUEL_ATI) * 1.09 - t1.SUBSIDY_ATI
    END AS AFTERTAX_INCOME_TOTAL,
    
    CASE 
      WHEN t2.is_xingjiang_flag = 0
      THEN t1.KPSR_ATI*1.09 + t1.FUEL_ATI*1.09 + t1.SUBSIDY_ATI*1.04 + t1.BRANCH_SUBSIDY + t1.XJ_SUBSIDY + t1.HYSR_ATI*1.09 + t1.HYBCSR_ATI*1.09 + t1.YZSR_ATI*1.09 + t1.TPSR_ATI * 1.06
      ELSE 38000 * t1.FT_TIME + t1.TPSR_ATI * 1.06 + t1.YZSR_ATI*1.09  - t1.SUBSIDY_ATI
    END AS PRETAX_INCOME_TOTAL
FROM t1 
left JOIN t2 
ON t1.JHWF_ELINECN2 = t2.JHWF_ELINECN2
AND t1.PLANE1 = t2.PLANE1
AND t1.JHWF_FLIGHT_NO = t2.JHWF_FLIGHT_NO
AND t1.HXXZ = t2.HXXZ
    '''
    return sql_str_airline_detail


def uatp_sql_str(startdate_str, enddate_str):
    sql_str_uatp_week = f'''
WITH t1 AS (
select
FLIGHT_NO,
concat(UP_LOCATION,DIS_LOCATION) segment,
avg(dist) dist
from air1.TB_FLIGHT_DETAIL
where DEP_DATE >= TO_CHAR(TRUNC(SYSDATE, 'YYYY'), 'YYYY-MM-DD')
group by concat(UP_LOCATION,DIS_LOCATION),FLIGHT_NO
),
t2 as (
    select
        count(PNAME) sum_sale_num
    from airext.PUSH_PAX_DETAIL
    where TK_DATE between {startdate_str} and {enddate_str}
    and fop like '%TP%' 
	and BOOKING_STATUS in ('HK','KK','RR','RL','UN','TK','DK') 
)

SELECT
    CASE
        WHEN b.AREA IN ('管总渠道销售','管总对外合作') THEN '管总'
        WHEN b.AREA IN ('北部','东部','西部','南部','新疆') THEN b.AREA
        ELSE '其他'
    END AS groupby_key,
	COUNT(a.PNAME),
	ROUND(COUNT(a.PNAME) / (SELECT sum_sale_num FROM t2),6) sale_rate,
    SUM(a.seg_fare) AS sum_seg_fare,
    ROUND(AVG(a.seg_fare), 6) AS avg_seg_fare,
    CASE
        WHEN SUM(t1.dist) = 0 OR SUM(t1.dist) IS NULL THEN 0
        ELSE round(sum(f.lst_pro_income - f.add_income) / sum(f.bkd*t1.dist),6)
    END AS RRPK,
    CASE
        WHEN COUNT(c.xingzhi) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN c.xingzhi = '独飞' THEN 1 END) / COUNT(c.xingzhi), 6)
    END AS dufei_rate,
    CASE
        WHEN COUNT(c.xingzhi) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN c.xingzhi = '弱竞争' THEN 1 END) / COUNT(c.xingzhi), 6)
    END AS ruojingzheng_rate,
    CASE
        WHEN COUNT(c.xingzhi) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN c.xingzhi = '强竞争' THEN 1 END) / COUNT(c.xingzhi), 6)
    END AS qiangjingzheng_rate,
	CASE
        WHEN COUNT(a.TK_DCP) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN a.TK_DCP <= 2 THEN 1 END) / COUNT(a.TK_DCP), 6)
    END AS day_rate1,
	CASE
        WHEN COUNT(a.TK_DCP) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN a.TK_DCP between 3 and 6 THEN 1 END) / COUNT(a.TK_DCP), 6)
    END AS day_rate2,
	CASE
        WHEN COUNT(a.TK_DCP) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN a.TK_DCP between 7 and 14 THEN 1 END) / COUNT(a.TK_DCP), 6)
    END AS day_rate3,
	CASE
        WHEN COUNT(a.TK_DCP) = 0 THEN 0
        ELSE ROUND(COUNT(CASE WHEN a.TK_DCP > 14 THEN 1 END) / COUNT(a.TK_DCP), 6)
    END AS day_rate4
FROM airext.PUSH_PAX_DETAIL a
LEFT JOIN air1.CHANNEL_management_detail b
    ON a.TK_AGENT = b.OFFICE_NUM
LEFT JOIN air1.DJ_HIS_DATA_BIG_TEMP_NOW c
    ON TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') = c.DEP_DATE
    AND CONCAT(a.UP_LOCATION, a.DIS_LOCATION) = c.segment
    AND SUBSTR(a.FLIGHT_NO, 3) = c.FLIGHT_NO
LEFT JOIN air1.TB_FLIGHT_DETAIL f
    ON TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') = f.DEP_DATE
    AND CONCAT(a.UP_LOCATION, a.DIS_LOCATION) = CONCAT(f.UP_LOCATION, f.DIS_LOCATION)
    AND SUBSTR(a.FLIGHT_NO, 3) = f.FLIGHT_NO
LEFT JOIN t1
    ON SUBSTR(a.FLIGHT_NO, 3) = t1.FLIGHT_NO
    AND CONCAT(a.UP_LOCATION, a.DIS_LOCATION) = t1.segment
WHERE a.fop LIKE '%TP%' 
    AND a.TK_DATE between {startdate_str} and {enddate_str}
    AND a.BOOKING_STATUS IN ('HK','KK','RR','RL','UN','TK','DK')
GROUP BY
    CASE
        WHEN b.AREA IN ('管总渠道销售','管总对外合作') THEN '管总'
        WHEN b.AREA IN ('北部','东部','西部','南部','新疆') THEN b.AREA
        ELSE '其他'
    END
    '''
    return sql_str_uatp_week