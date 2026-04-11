with t1 as(
		select
    dep_date,
    case when PLANE like '9%' then 'C909'
    else '空客' end as plane_type,
    sum(BKD) as total_bkd,
    round(sum(BKD*DIST)/sum(DISCAP*DIST),4) as rsk,
    round(sum(case when FLTTYPE <> 'I' then lst_pro_income-add_income else 0 end)/nullif(sum(case when FLTTYPE <> 'I' then BKD else 0 end),0)) as avg_price
    from AIR1.TB_FLIGHT_DETAIL
    where dep_date between '2026-02-02' and to_char(sysdate-1,'YYYY-MM-DD')
    group by dep_date,
    case when PLANE like '9%' then 'C909'
    else '空客' end

    union all

    select
    '2月2日-'||EXTRACT(MONTH FROM sysdate-1)||'月'||EXTRACT(DAY FROM sysdate-1)||'日' as dep_date,
    case when PLANE like '9%' then 'C909'
    else '空客' end as plane_type,
    sum(BKD) as total_bkd,
    round(sum(BKD*DIST)/sum(DISCAP*DIST),4) as rsk,
    round(sum(case when FLTTYPE <> 'I' then lst_pro_income-add_income else 0 end)/nullif(sum(case when FLTTYPE <> 'I' then BKD else 0 end),0)) as avg_price
    from AIR1.TB_FLIGHT_DETAIL
    where dep_date between '2026-02-02' and to_char(sysdate-1,'YYYY-MM-DD')
    group by case when PLANE like '9%' then 'C909'
    else '空客' end
		order by dep_date
),

t2 as (
		select
    dep_date,
		'公司整体' as plane_type,
    sum(BKD) as total_bkd,
    round(sum(BKD*DIST)/sum(DISCAP*DIST),4) as rsk,
    round(sum(case when FLTTYPE <> 'I' then lst_pro_income-add_income else 0 end)/nullif(sum(case when FLTTYPE <> 'I' then BKD else 0 end),0)) as avg_price
    from AIR1.TB_FLIGHT_DETAIL
    where dep_date between '2026-02-02' and to_char(sysdate-1,'YYYY-MM-DD')
    group by dep_date

		union all

		select
    '2月2日-'||EXTRACT(MONTH FROM sysdate-1)||'月'||EXTRACT(DAY FROM sysdate-1)||'日' as dep_date,
		'公司整体' as plane_type,
    sum(BKD) as total_bkd,
    round(sum(BKD*DIST)/sum(DISCAP*DIST),4) as rsk,
    round(sum(case when FLTTYPE <> 'I' then lst_pro_income-add_income else 0 end)/nullif(sum(case when FLTTYPE <> 'I' then BKD else 0 end),0)) as avg_price
    from AIR1.TB_FLIGHT_DETAIL
    where dep_date between '2026-02-02' and to_char(sysdate-1,'YYYY-MM-DD')
    order by dep_date
)

SELECT
    t1.dep_date,
    MAX(CASE WHEN t1.plane_type = '空客' THEN t1.total_bkd END) as 空客_total_bkd,
    MAX(CASE WHEN t1.plane_type = '空客' THEN t1.rsk END) as 空客_rsk,
    MAX(CASE WHEN t1.plane_type = '空客' THEN t1.avg_price END) as 空客_avg_price,
    MAX(CASE WHEN t1.plane_type = 'C909' THEN t1.total_bkd END) as C909_total_bkd,
    MAX(CASE WHEN t1.plane_type = 'C909' THEN t1.rsk END) as C909_rsk,
    MAX(CASE WHEN t1.plane_type = 'C909' THEN t1.avg_price END) as C909_avg_price,
	MAX(CASE WHEN t2.plane_type = '公司整体' THEN t2.total_bkd END) as 公司整体_total_bkd,
    MAX(CASE WHEN t2.plane_type = '公司整体' THEN t2.rsk END) as 公司整体_rsk,
    MAX(CASE WHEN t2.plane_type = '公司整体' THEN t2.avg_price END) as 公司整体_avg_price
FROM t1 left join t2
on t1.dep_date = t2.dep_date
GROUP BY t1.dep_date
ORDER BY
    t1.dep_date
