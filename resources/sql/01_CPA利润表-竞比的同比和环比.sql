-- ---------------------------------------------一、日累计：单程--------------------------------------
select * from
(
-- ----------------------本期-----将南航需要给成都航的收入加在成都航收入之上----------------------
with t1 as(
select
'1.日累计-单程' leibie,
plane,
'单程' eline_leibie,
AREA_REV area,
p_in_charge,
dep_date,
dep_week,
'EU'||p.flight_no flight_no,
t.last_flight_no,
eline,
p.segment,
'' time_prd,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER,
sum(at_time) at_time,
sum(ask) ask,
sum(rpk) rpk,
avg(share_cap) share_cap,
avg(dist) dist,
avg(full_price) full_price
from air1.DJ_HIS_DATA_BIG_TEMP_NOW p
left join air1.DJ_flight_no_duibiao_tongqi t on p.dep_date between t.st_date and t.ed_date and p.segment=t.segment and p.flight_no=t.flight_no
where dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and 'EU'||p.flight_no in(select distinct flight_no from airext.TB_EU_CZ_CPA_FLIGHT c where p.dep_date = c.st_date)

group by
plane,
AREA_REV,
p_in_charge,
dep_date,
dep_week,
'EU'||p.flight_no,t.last_flight_no,
eline,
p.segment

),

-- --------------------------------按成都航销售的座位数乘以2，作为按成都航的销售能力还原的数据--------------------
t2 as
(select
DEP_DATE,
FLIGHT_NO,
SEGMENT,
FLIGHT_NO_CZ,
sum(bkd_y*2+bkd_p) bkd,
sum(pro_income_y*2+pro_income_p) LST_PRO_INCOME,
sum(kuisun) kuisun,
sum(fencheng) fencheng,
sum(bucha) bucha,
sum(pro_income_cztoeu) pro_income_cztoeu,

sum(cap_p) cap_p_EU,
sum(cap_y) cap_y_EU,
sum(CAP_Y_BUJU) CAP_Y_BUJU,
sum(CAP_p_BUJU) CAP_p_BUJU,
sum(BKD_Y+BKD_P) bkd_all_eu,
sum(BKD_Y) bkd_y_eu,
sum(BKD_p) bkd_p_eu,
sum(PRO_INCOME) PRO_INCOME_cj,
sum(PRO_INCOME_y) PRO_INCOME_y,
sum(PRO_INCOME_p) PRO_INCOME_p,
sum(add_income_y) add_income_y,
sum(add_income_p) add_income_p,

round(avg(CAP_Y_CZ),0) CAP_Y_CZ_cj,
round(avg(BKD_CZ),0) BKD_Y_CZ_cj,
sum(CAP_Y_CZ) CAP_Y_CZ,
sum(BKD_CZ) BKD_Y_CZ,
sum(PRO_INCOME_CZ) PRO_INCOME_CZ

from air1.dj_cpa_proincome_restore
where FLIGHT_NO_CZ is not null
group by
DEP_DATE,
FLIGHT_NO,
SEGMENT,
FLIGHT_NO_CZ),


--  -----------同期数据--------
t3 as
(select
dep_date_now dep_date,
flight_no,
segment,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_last l
where dep_date_now between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')

group by
dep_date_now,
flight_no,
segment
order by dep_date
),

--  -----------环期数据--------
t4 as
(select
to_char(to_date(dep_date, 'YYYY-MM-DD') + 7, 'YYYY-MM-DD') dep_date,
'EU'||flight_no flight_no,
segment,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_now
where dep_date between '2025-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
group by
dep_date,
flight_no,
segment
order by dep_date
),

--  -----------竞比数据--------
t5 as
(select
a.dep_date dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
a.dep_date
),

--  -----------竞比环期数据--------
t6 as
(select
to_char(to_date(a.dep_date, 'YYYY-MM-DD') + 7, 'YYYY-MM-DD') dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-08-01' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2025-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
a.dep_date
),

--  -----------竞比同期数据--------
t7 as
(select
b.dep_date dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
left join air1.SJDY b
on a.dep_date = b.last_dep_date
where a.dep_date between '2024-08-01' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2024-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
b.dep_date
)

select
t1.LEIBIE,t1.PLANE,t1.ELINE_LEIBIE,t1.AREA,t1.P_IN_CHARGE,
t1.DEP_DATE,substr(t1.DEP_DATE,0,4) as dep_year,t1.DEP_WEEK,t1.FLIGHT_NO,t1.ELINE,
t1.seg_count,round(t2.bkd/t1.seg_count,0) bkd_avg,
round((t2.bkd*t1.dist)/t1.ask,3)  plf,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd),0) price,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd)/t1.full_price,2) rate,
round(t2.LST_PRO_INCOME/decode(t2.bkd*t1.dist,0,null,t2.bkd*t1.dist),3) rrpk,
round(t2.LST_PRO_INCOME/t1.ask,3) rask,
round(t2.LST_PRO_INCOME/10000,0) LST_PRO_INCOME,
round(t2.LST_PRO_INCOME/t1.at_time/10000,2) LST_PRO_INCOME_per,
t1.plf plf_cpa,
t1.LST_PRO_INCOME LST_PRO_INCOME_cpa,
t1.rask rask_cpa,
round(t1.LST_PRO_INCOME/t1.at_time,2) PRO_INCOME_per_cpa,
round(t2.fencheng/10000,2) fencheng,
round(t2.pro_income_cztoeu/10000,2) pro_income_cztoeu,
round(t2.bucha/10000,2) bucha,
-- 按算法还原同比----------
t1.seg_count-t3.seg_count seg_count_tongbi,
round((t2.bkd*t1.dist)/t1.ask,3)-t3.plf plf_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t2.bkd,0)-t3.price price_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/(t2.bkd*t1.dist),3)-t3.rrpk rrpk_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t1.ask,3)-t3.rask rask_tongbi,
round(t2.LST_PRO_INCOME/10000,0)-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi,

-- -按结算还原同比(承运)--------------------
t1.plf-t3.plf plf_tongbi_c,
round(t1.price-t3.price,0) price_tongbi_h,
t1.rrpk-t3.rrpk rrpk_tongbi_h,
t1.rask-t3.rask rask_tongbi_h,
t1.LST_PRO_INCOME-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi_h,

-- -按结算还原环比(承运)--------------------
t1.plf-t4.plf plf_huanbi_c,
t1.price-t4.price price_huanbi_h,
t1.rrpk-t4.rrpk rrpk_huanbi_h,
t1.rask-t4.rask rask_huanbi_h,
t1.LST_PRO_INCOME-t4.LST_PRO_INCOME LST_PRO_INCOME_huanbi_h,

-- -按结算还原竞比行业(承运)--------------------
t1.plf-t5.plf plf_jingbi_c,
round(t1.price-t5.price, 0) price_jingbi_h,
t1.rrpk-t5.rrpk rrpk_jingbi_h,
t1.rask-t5.rask rask_jingbi_h,


-- -按结算还原竞比的环比(承运)--------------------
t1.plf-t4.plf-(t5.plf-t6.plf) plf_jbhb_c,
round(t1.price-t4.price-(t5.price-t6.price), 0) price_jbhb_h,
t1.rrpk-t4.rrpk-(t5.rrpk-t6.rrpk) rrpk_jbhb_h,
t1.rask-t4.rask-(t5.rask-t6.rask) rask_jbhb_h,

-- -按结算还原竞比的同比(承运)--------------------
t1.plf-t3.plf-(t5.plf-t7.plf) plf_jbtb_c,
round(t1.price-t3.price-(t5.price-t7.price), 0) price_jbtb_h,
t1.rrpk-t3.rrpk-(t5.rrpk-t7.rrpk) rrpk_jbtb_h,
t1.rask-t3.rask-(t5.rask-t7.rask) rask_jbtb_h,

-- -----------EU参与结算数据-------------------------
round(t2.cap_y_EU+t2.cap_p_EU,0) cap_EU_cj,
round(t2.CAP_Y_BUJU+t2.CAP_p_BUJU,0) CAP_BUJU,
round(t2.bkd_all_eu,0) bkd_all_eu,
round(t2.bkd_all_eu,0) bkd_eu_cj,
round(t2.bkd_all_eu/(t2.cap_y_EU+t2.cap_p_EU),3) plf_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.bkd_y_eu+t2.bkd_p_eu,0),0) price_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.cap_y_EU+t2.cap_p_EU,0),0) price_eu_cap,
round(t2.PRO_INCOME_cj/10000,2) PRO_INCOME_cj,
-- --------------南航参与结算数据------------------------------------
t2.FLIGHT_NO_CZ,
t2.CAP_Y_CZ_cj,
t2.BKD_Y_CZ_cj,
round(t2.BKD_Y_CZ/t2.CAP_Y_CZ,3) plf_cz,
round(t2.PRO_INCOME_CZ/t2.BKD_Y_CZ,0) price_cz,
round(t2.PRO_INCOME_CZ/t2.cap_Y_CZ,0) price_cz_cap,
round(t2.PRO_INCOME_CZ/10000,2) PRO_INCOME_CZ

from t1
left join t2 on t1.segment=t2.segment and t1.flight_no=t2.flight_no and t1.dep_date=t2.dep_date
left join t3 on t1.segment=t3.segment and t1.last_flight_no=t3.flight_no and t1.dep_date=t3.dep_date
left join t4 on t1.segment=t4.segment and t1.flight_no=t4.flight_no and t1.dep_date=t4.dep_date
left join t5 on t1.segment=t5.segment and t1.dep_date=t5.dep_date
left join t6 on t1.segment=t6.segment and t1.dep_date=t6.dep_date
left join t7 on t1.segment=t7.segment and t1.dep_date=t7.dep_date
order by t1.dep_date
)

union all

---------------------------------------------二、月累计：单程--------------------------------------
select * from
(
-- ----------------------本期-----将南航需要给成都航的收入加在成都航收入之上----------------------
with t1 as(
select
'2.月累计-单程' leibie,
case when substr(plane,1,1)='3' then '空客'
when substr(plane,1,1)='9' then '909'
else plane end plane,
'单程' eline_leibie,
AREA_REV area,
p_in_charge,
substr(dep_date,0,4) dep_year,
substr(dep_date,6,2)||'月' dep_date,
'' dep_week,
'EU'||p.flight_no flight_no,
eline,
p.segment,
'' time_prd,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER,
sum(at_time) at_time,
sum(ask) ask,
sum(rpk) rpk,
avg(share_cap) share_cap,
avg(dist) dist,
avg(full_price) full_price
from air1.DJ_HIS_DATA_BIG_TEMP_NOW p
left join air1.DJ_flight_no_duibiao_tongqi t on p.dep_date between t.st_date and t.ed_date and p.segment=t.segment and p.flight_no=t.flight_no
where dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and 'EU'||p.flight_no in(select distinct flight_no from airext.TB_EU_CZ_CPA_FLIGHT c where p.dep_date = c.st_date)

group by
case when substr(plane,1,1)='3' then '空客'
when substr(plane,1,1)='9' then '909'
else plane end,
AREA_REV,
p_in_charge,
'EU'||p.flight_no,
eline,
p.segment,substr(dep_date,0,4),substr(dep_date,6,2)
),

-- --------------------------------按成都航销售的座位数乘以2，作为按成都航的销售能力还原的数据--------------------
t2 as
(select
substr(dep_date,6,2)||'月' DEP_DATE,
FLIGHT_NO FLIGHT_NO,
SEGMENT,
FLIGHT_NO_CZ,
sum(bkd_y*2+bkd_p) bkd,
sum(pro_income_y*2+pro_income_p) LST_PRO_INCOME,
sum(kuisun) kuisun,
sum(fencheng) fencheng,
sum(bucha) bucha,
sum(pro_income_cztoeu) pro_income_cztoeu,

sum(cap_p) cap_p_EU,
sum(cap_y) cap_y_EU,
sum(CAP_Y_BUJU) CAP_Y_BUJU,
sum(CAP_p_BUJU) CAP_p_BUJU,
sum(BKD_Y+BKD_P) bkd_all_eu,
sum(BKD_Y) bkd_y_eu,
sum(BKD_p) bkd_p_eu,
sum(PRO_INCOME) PRO_INCOME_cj,
sum(PRO_INCOME_y) PRO_INCOME_y,
sum(PRO_INCOME_p) PRO_INCOME_p,
sum(add_income_y) add_income_y,
sum(add_income_p) add_income_p,

round(avg(CAP_Y_CZ),0) CAP_Y_CZ_cj,
round(avg(BKD_CZ),0) BKD_Y_CZ_cj,
sum(CAP_Y_CZ) CAP_Y_CZ,
sum(BKD_CZ) BKD_Y_CZ,
sum(PRO_INCOME_CZ) PRO_INCOME_CZ

from air1.dj_cpa_proincome_restore_sum
where LST_PRO_INCOME_ALL is not null
group by
FLIGHT_NO,
SEGMENT,FLIGHT_NO_CZ,substr(dep_date,6,2)
),


--  -----------同期数据--------
t3 as
(select
substr(dep_date,6,2)||'月' dep_date,
'EU'||flight_no flight_no,
segment,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_last l
where dep_date_now between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')

group by
flight_no,
segment,substr(dep_date,6,2)
),

--  -----------环期数据（上月）--------
t4 as
(select
substr(to_char(add_months(to_date(dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月' dep_date,
'EU'||flight_no flight_no,
segment,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_now
where dep_date between '2025-08-01' and to_char(LAST_DAY(ADD_MONTHS(SYSDATE, -1)), 'YYYY-MM-DD')
group by
flight_no,
segment,
substr(to_char(add_months(to_date(dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月'
),

--  -----------竞比数据（月累计）--------
t5 as
(select
to_char(to_date(a.dep_date,'yyyy-mm-dd'), 'MM')||'月' dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*a.dist)/sum(a.share_cap*a.dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*a.price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk,
round(sum(a.PRO_INCOME)/10000,2) LST_PRO_INCOME
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
to_char(to_date(a.dep_date,'yyyy-mm-dd'), 'MM')||'月'
),

--  -----------竞比环期数据（月累计）--------
t6 as
(select
substr(to_char(add_months(to_date(a.dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月' dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-08-01' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2025-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
substr(to_char(add_months(to_date(a.dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月'
),

--  -----------竞比同期数据--------
t7 as
(select
to_char(to_date(b.dep_date,'yyyy-mm-dd'), 'MM')||'月' dep_date,
a.up_location||a.dis_location segment,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
left join air1.SJDY b
on a.dep_date = b.last_dep_date
where a.dep_date between '2024-08-01' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2024-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
a.up_location||a.dis_location,
to_char(to_date(b.dep_date,'yyyy-mm-dd'), 'MM')||'月'
)

select
t1.LEIBIE,t1.PLANE,t1.ELINE_LEIBIE,t1.AREA,t1.P_IN_CHARGE,
t1.DEP_DATE,t1.dep_year,t1.DEP_WEEK,t1.FLIGHT_NO,t1.ELINE,
t1.seg_count,round(t2.bkd/t1.seg_count,0) bkd_avg,
round((t2.bkd*t1.dist)/t1.ask,3)  plf,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd),0) price,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd)/t1.full_price,2) rate,
round(t2.LST_PRO_INCOME/decode(t2.bkd*t1.dist,0,null,t2.bkd*t1.dist),3) rrpk,
round(t2.LST_PRO_INCOME/t1.ask,3) rask,
round(t2.LST_PRO_INCOME/10000,0) LST_PRO_INCOME,
round(t2.LST_PRO_INCOME/t1.at_time/10000,2) LST_PRO_INCOME_per,
t1.plf plf_cpa,
t1.LST_PRO_INCOME LST_PRO_INCOME_cpa,
t1.rask rask_cpa,
round(t1.LST_PRO_INCOME/t1.at_time,2) PRO_INCOME_per_cpa,
round(t2.fencheng/10000,2) fencheng,
round(t2.pro_income_cztoeu/10000,2) pro_income_cztoeu,
round(t2.bucha/10000,2) bucha,
-- 按算法还原同比----------
t1.seg_count-t3.seg_count seg_count_tongbi,
round((t2.bkd*t1.dist)/t1.ask,3)-t3.plf plf_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t2.bkd,0)-t3.price price_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/(t2.bkd*t1.dist),3)-t3.rrpk rrpk_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t1.ask,3)-t3.rask rask_tongbi,
round(t2.LST_PRO_INCOME/10000,0)-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi,

-- -按结算还原同比(承运)--------------------
t1.plf-t3.plf plf_tongbi_c,
round(t1.price-t3.price,0) price_tongbi_h,
t1.rrpk-t3.rrpk rrpk_tongbi_h,
t1.rask-t3.rask rask_tongbi_h,
t1.LST_PRO_INCOME-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi_h,

-- -按结算还原环比(承运)--------------------
t1.plf-t4.plf plf_huanbi_c,
t1.price-t4.price price_huanbi_h,
t1.rrpk-t4.rrpk rrpk_huanbi_h,
t1.rask-t4.rask rask_huanbi_h,
t1.LST_PRO_INCOME-t4.LST_PRO_INCOME LST_PRO_INCOME_huanbi_h,

-- -按结算还原竞比行业(承运)--------------------
t1.plf-t5.plf plf_jingbi_c,
round(t1.price-t5.price, 0) price_jingbi_h,
t1.rrpk-t5.rrpk rrpk_jingbi_h,
t1.rask-t5.rask rask_jingbi_h,

-- -按结算还原竞比的环比(承运)--------------------
t1.plf-t4.plf-(t5.plf-t6.plf) plf_jbhb_c,
round(t1.price-t4.price-(t5.price-t6.price), 0) price_jbhb_h,
t1.rrpk-t4.rrpk-(t5.rrpk-t6.rrpk) rrpk_jbhb_h,
t1.rask-t4.rask-(t5.rask-t6.rask) rask_jbhb_h,

-- -按结算还原竞比的同比(承运)--------------------
t1.plf-t3.plf-(t5.plf-t7.plf) plf_jbtb_c,
round(t1.price-t3.price-(t5.price-t7.price), 0) price_jbtb_h,
t1.rrpk-t3.rrpk-(t5.rrpk-t7.rrpk) rrpk_jbtb_h,
t1.rask-t3.rask-(t5.rask-t7.rask) rask_jbtb_h,

-- -----------EU参与结算数据-------------------------
round(t2.cap_y_EU+t2.cap_p_EU,0) cap_EU_cj,
round(t2.CAP_Y_BUJU+t2.CAP_p_BUJU,0) CAP_BUJU,
round(t2.bkd_all_eu,0) bkd_all_eu,
round(t2.bkd_all_eu,0) bkd_eu_cj,
round(t2.bkd_all_eu/(t2.cap_y_EU+t2.cap_p_EU),3) plf_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.bkd_y_eu+t2.bkd_p_eu,0),0) price_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.cap_y_EU+t2.cap_p_EU,0),0) price_eu_cap,
round(t2.PRO_INCOME_cj/10000,2) PRO_INCOME_cj,
-- --------------南航参与结算数据------------------------------------
t2.FLIGHT_NO_CZ,
t2.CAP_Y_CZ_cj,
t2.BKD_Y_CZ_cj,
round(t2.BKD_Y_CZ/t2.CAP_Y_CZ,3) plf_cz,
round(t2.PRO_INCOME_CZ/t2.BKD_Y_CZ,0) price_cz,
round(t2.PRO_INCOME_CZ/t2.cap_Y_CZ,0) price_cz_cap,
round(t2.PRO_INCOME_CZ/10000,2) PRO_INCOME_CZ


from t1
left join t2 on t1.segment=t2.segment and t1.flight_no=t2.flight_no  and t1.dep_date=t2.dep_date
left join t3 on t1.segment=t3.segment and t1.flight_no=t3.flight_no  and t1.dep_date=t3.dep_date
left join t4 on t1.segment=t4.segment and t1.flight_no=t4.flight_no  and t1.dep_date=t4.dep_date
left join t5 on t1.segment=t5.segment and t1.dep_date=t5.dep_date
left join t6 on t1.segment=t6.segment and t1.dep_date=t6.dep_date
left join t7 on t1.segment=t7.segment and t1.dep_date=t7.dep_date
)

union all

-----------------------------------------------三、月累计：机型-----------------------------------------
select * from
(
-- ----------------------本期-----将南航需要给成都航的收入加在成都航收入之上----------------------
with t1 as(
select
'3.月累计-机型' leibie,
case
when plane in ('909','90B','AJ27') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end plane,
'分机型' eline_leibie,
'' area,
'' p_in_charge,
substr(dep_date,0,4) dep_year,
substr(dep_date,6,2)||'月'  dep_date,
'' dep_week,
'' flight_no,
'' eline,
'' segment,
'' time_prd,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER,
sum(at_time) at_time,
sum(ask) ask,
sum(rpk) rpk,
avg(share_cap) share_cap,
avg(dist) dist,
avg(full_price) full_price
from air1.DJ_HIS_DATA_BIG_TEMP_NOW p
where dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and 'EU'||flight_no in(select distinct flight_no from airext.TB_EU_CZ_CPA_FLIGHT c where p.dep_date=c.st_date)

group by
case
when plane in ('909','90B','AJ27') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end,substr(dep_date,0,4),substr(dep_date,6,2) ),

-- --------------------------------按成都航销售的座位数乘以2，作为按成都航的销售能力还原的数据--------------------
t2 as
(select
substr(dep_date,6,2)||'月'  DEP_DATE,
case
when plane in ('909','90B','AJ27') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321','A319','A320','A321') then '空客'
else plane end plane,
'' FLIGHT_NO,
'' SEGMENT,
'' FLIGHT_NO_CZ,
sum(bkd_y*2+bkd_p) bkd,
sum(pro_income_y*2+pro_income_p) LST_PRO_INCOME,
sum(kuisun) kuisun,
sum(fencheng) fencheng,
sum(bucha) bucha,
sum(pro_income_cztoeu) pro_income_cztoeu,

sum(cap_p) cap_p_EU,
sum(cap_y) cap_y_EU,
sum(CAP_Y_BUJU) CAP_Y_BUJU,
sum(CAP_p_BUJU) CAP_p_BUJU,
sum(BKD_Y+BKD_P) bkd_all_eu,
sum(BKD_Y) bkd_y_eu,
sum(BKD_p) bkd_p_eu,
sum(PRO_INCOME) PRO_INCOME_cj,
sum(PRO_INCOME_y) PRO_INCOME_y,
sum(PRO_INCOME_p) PRO_INCOME_p,
sum(add_income_y) add_income_y,
sum(add_income_p) add_income_p,

round(avg(CAP_Y_CZ),0) CAP_Y_CZ_cj,
round(avg(BKD_CZ),0) BKD_Y_CZ_cj,
sum(CAP_Y_CZ) CAP_Y_CZ,
sum(BKD_CZ) BKD_Y_CZ,
sum(PRO_INCOME_CZ) PRO_INCOME_CZ

from air1.dj_cpa_proincome_restore_sum
where LST_PRO_INCOME_ALL is not null
and FLIGHT_NO_CZ is not null
group by
case
when plane in ('909','90B','AJ27') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321','A319','A320','A321') then '空客'
else plane end,substr(dep_date,6,2)),


--  -----------同期数据--------
t3 as
(select
substr(dep_date,6,2)||'月' dep_date,
case
when plane in('909','909') then '909'
when plane in('319','320','321') then '空客'
else plane end plane,
'' flight_no,
'' segment,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_last l
where dep_date_now between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
-- and 'EU'||flight_no in(select distinct flight_no from airext.TB_EU_CZ_CPA_FLIGHT c where l.dep_date_now = c.st_date)
group by
case
when plane in('909','909') then '909'
when plane in('319','320','321') then '空客'
else plane end,substr(dep_date,6,2)
),

--  -----------环期数据（上月）--------
t4 as
(select
substr(to_char(add_months(to_date(dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月' dep_date,
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end plane,
sum(seg_count) seg_count,
sum(bkd) bkd,
round(sum(rpk)/decode(sum(ask),0,null,sum(ask)),3) plf,
round(sum(PRO_INCOME-add_income)/decode(sum(bkd),0,null,sum(bkd)),0) price,
round(avg(RATE_NOW),2) rate,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(rpk),0,null,sum(rpk)),4) rrpk,
round(sum(LST_PRO_INCOME-add_income)/decode(sum(ask),0,null,sum(ask)),4) rask,
round(sum(LST_PRO_INCOME)/10000,2) LST_PRO_INCOME,
round(sum(LST_PRO_INCOME)/decode(sum(at_time),0,null,sum(at_time)),2) LST_PRO_INCOME_PER
from air1.DJ_HIS_DATA_BIG_TEMP_now
where dep_date between '2025-08-01' and to_char(sysdate-1-interval '1' month,'yyyy-mm-dd')
and 'EU'||flight_no in(select distinct flight_no from airext.TB_EU_CZ_CPA_FLIGHT c where dep_date = c.st_date)
group by
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end, substr(to_char(add_months(to_date(dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月'
),

--  -----------竞比行业数据--------
t5 as
(select
substr(a.dep_date,6,2)||'月' dep_date,
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end plane,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk,
round(sum(a.PRO_INCOME)/10000,2) LST_PRO_INCOME
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2025-09-15' and to_char(sysdate-1,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
substr(a.dep_date,6,2),
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end
),

--  -----------竞比环期数据（月累计）--------
t6 as
(select
substr(to_char(add_months(to_date(a.dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月' dep_date,
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end plane,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
where a.dep_date between '2025-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and c.st_date between '2025-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
substr(to_char(add_months(to_date(a.dep_date, 'YYYY-MM-DD'), 1), 'YYYY-MM-DD'),6,2)||'月',
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end
),

--  -----------竞比同期数据--------
t7 as
(select
to_char(to_date(b.dep_date,'yyyy-mm-dd'), 'MM')||'月' dep_date,
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end plane,
round(avg(a.bkd),0) bkd,
round(sum(a.bkd*dist)/sum(share_cap*dist),3) plf,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd),0,null,sum(a.bkd)),2) price,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.price),0,null,sum(a.bkd*price)),2) rate,
round(sum(a.pro_income-a.add_income)/decode(sum(a.share_cap*a.dist),0,null,sum(a.share_cap*a.dist)),4) rask,
round(sum(a.pro_income-a.add_income)/decode(sum(a.bkd*a.dist),0,null,sum(a.bkd*a.dist)),4) rrpk
from airext.TB_DEPT_SY_DETAIL a
inner join airext.TB_EU_CZ_CPA_FLIGHT c
on a.up_location||a.dis_location=c.up_location||c.dis_location
left join air1.SJDY b
on a.dep_date = b.last_dep_date
where a.dep_date between '2024-08-01' and to_char(sysdate-1,'yyyy-mm-dd')
and c.st_date between '2024-08-01' and to_char(sysdate-1-7,'yyyy-mm-dd')
and a.bkd <> 0
and a.pro_income <> 0
group by
to_char(to_date(b.dep_date,'yyyy-mm-dd'), 'MM')||'月',
case
when plane in ('909','90B','AJ27','ARJ') then '909'
when plane in ('32Y','32T','32W','319','32X','32V','320','321') then '空客'
else plane end
)

select
t1.LEIBIE,t1.PLANE,t1.ELINE_LEIBIE,t1.AREA,t1.P_IN_CHARGE,
t1.DEP_DATE,t1.dep_year,t1.DEP_WEEK,t1.FLIGHT_NO,t1.ELINE,
t1.seg_count,round(t2.bkd/t1.seg_count,0) bkd_avg,
round((t2.bkd*t1.dist)/t1.ask,3)  plf,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd),0) price,
round(t2.LST_PRO_INCOME/decode(t2.bkd,0,null,t2.bkd)/t1.full_price,2) rate,
round(t2.LST_PRO_INCOME/decode(t2.bkd*t1.dist,0,null,t2.bkd*t1.dist),3) rrpk,
round(t2.LST_PRO_INCOME/t1.ask,3) rask,
round(t2.LST_PRO_INCOME/10000,0) LST_PRO_INCOME,
round(t2.LST_PRO_INCOME/t1.at_time/10000,2) LST_PRO_INCOME_per,
t1.plf plf_cpa,
t1.LST_PRO_INCOME LST_PRO_INCOME_cpa,
t1.rask rask_cpa,
round(t1.LST_PRO_INCOME/t1.at_time,2) PRO_INCOME_per_cpa,
round(t2.fencheng/10000,2) fencheng,
round(t2.pro_income_cztoeu/10000,2) pro_income_cztoeu,
round(t2.bucha/10000,2) bucha,
-- 按算法还原同比----------
t1.seg_count-t3.seg_count seg_count_tongbi,
round((t2.bkd*t1.dist)/t1.ask,3)-t3.plf plf_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t2.bkd,0)-t3.price price_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/(t2.bkd*t1.dist),3)-t3.rrpk rrpk_tongbi,
round((t2.LST_PRO_INCOME-t2.add_income_y*2-t2.add_income_p)/t1.ask,3)-t3.rask rask_tongbi,
round(t2.LST_PRO_INCOME/10000,0)-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi,

-- -按结算还原同比(承运)--------------------
t1.plf-t3.plf plf_tongbi_c,
round(t1.price-t3.price,0) price_tongbi_h,
t1.rrpk-t3.rrpk rrpk_tongbi_h,
t1.rask-t3.rask rask_tongbi_h,
t1.LST_PRO_INCOME-t3.LST_PRO_INCOME LST_PRO_INCOME_tongbi_h,

-- -按结算还原环比(承运)--------------------
t1.plf-t4.plf plf_huanbi_c,
t1.price-t4.price price_huanbi_h,
t1.rrpk-t4.rrpk rrpk_huanbi_h,
t1.rask-t4.rask rask_huanbi_h,
t1.LST_PRO_INCOME-t4.LST_PRO_INCOME LST_PRO_INCOME_huanbi_h,

-- -按结算还原竞比行业(承运)--------------------
t1.plf-t5.plf plf_jingbi_c,
round(t1.price-t5.price, 0) price_jingbi_h,
t1.rrpk-t5.rrpk rrpk_jingbi_h,
t1.rask-t5.rask rask_jingbi_h,

-- -按结算还原竞比的环比(承运)--------------------
t1.plf-t4.plf-(t5.plf-t6.plf) plf_jbhb_c,
round(t1.price-t4.price-(t5.price-t6.price), 0) price_jbhb_h,
t1.rrpk-t4.rrpk-(t5.rrpk-t6.rrpk) rrpk_jbhb_h,
t1.rask-t4.rask-(t5.rask-t6.rask) rask_jbhb_h,

-- -按结算还原竞比的同比(承运)--------------------
t1.plf-t3.plf-(t5.plf-t7.plf) plf_jbtb_c,
round(t1.price-t3.price-(t5.price-t7.price), 0) price_jbtb_h,
t1.rrpk-t3.rrpk-(t5.rrpk-t7.rrpk) rrpk_jbtb_h,
t1.rask-t3.rask-(t5.rask-t7.rask) rask_jbtb_h,

-- -----------EU参与结算数据-------------------------
round(t2.cap_y_EU+t2.cap_p_EU,0) cap_EU_cj,
round(t2.CAP_Y_BUJU+t2.CAP_p_BUJU,0) CAP_BUJU,
round(t2.bkd_all_eu,0) bkd_all_eu,
round(t2.bkd_all_eu,0) bkd_eu_cj,
round(t2.bkd_all_eu/(t2.cap_y_EU+t2.cap_p_EU),3) plf_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.bkd_y_eu+t2.bkd_p_eu,0),0) price_eu_cj,
round(t2.PRO_INCOME_cj/nullif(t2.cap_y_EU+t2.cap_p_EU,0),0) price_eu_cap,
round(t2.PRO_INCOME_cj/10000,2) PRO_INCOME_cj,
-- --------------南航参与结算数据------------------------------------
t2.FLIGHT_NO_CZ,
t2.CAP_Y_CZ_cj,
t2.BKD_Y_CZ_cj,
round(t2.BKD_Y_CZ/t2.CAP_Y_CZ,3) plf_cz,
round(t2.PRO_INCOME_CZ/t2.BKD_Y_CZ,0) price_cz,
round(t2.PRO_INCOME_CZ/t2.cap_Y_CZ,0) price_cz_cap,
round(t2.PRO_INCOME_CZ/10000,2) PRO_INCOME_CZ


from t1
left join t2 on t1.plane=t2.plane and t1.dep_date=t2.dep_date
left join t3 on t1.plane=t3.plane and t1.dep_date=t3.dep_date
left join t4 on t1.plane=t4.plane and t1.dep_date=t4.dep_date
left join t5 on t1.plane=t5.plane and t1.dep_date=t5.dep_date
left join t6 on t1.plane=t6.plane and t1.dep_date=t6.dep_date
left join t7 on t1.plane=t7.plane and t1.dep_date=t7.dep_date
order by plane desc, dep_date
)
