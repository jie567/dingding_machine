with t1 as(
select
FLIGHT_NO,
concat(UP_LOCATION,DIS_LOCATION) segment,
avg(dist) dist
from air1.TB_FLIGHT_DETAIL
where DEP_DATE >= TO_CHAR(TRUNC(SYSDATE, 'YYYY'), 'YYYY-MM-DD')
group by FLIGHT_NO,concat(UP_LOCATION,DIS_LOCATION)
)

select
b.IATA_NUM,
b.OFFICE_NUM,
b.SALES_CHANNEL_NAME_FULL,
a.TK_NUM,
TO_CHAR(a.TK_DATE, 'YYYY-MM-DD') TK_DATE,
TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') DEP_DATE,
a.TK_DCP,
a.SD_CLASS,
a.ORASD_CLASS,
concat(a.UP_LOCATION,a.DIS_LOCATION)E_LINE,
round(t1.dist,3) dist,
c.xingzhi,
a.FLIGHT_NO,
d.MAR_AREA,
a.PASSENGER_TYPE,
a.PNAME,
a.seg_fare,
a.SEG_ADDFARE,
a.SEG_TAX,
'TP',
REGEXP_SUBSTR(fop, 'TP/([^/]+)', 1, 1, NULL, 1) AS UATP_num,
e.PLF,
round((f.lst_pro_income-f.add_income)/NULLIF(f.bkd, 0),3) avg_price,
'',
'',
b.SALES_CHANNEL_TYPE,
b.AREA
from airext.PUSH_PAX_DETAIL a

left join air1.CHANNEL_management_detail b
on a.TK_AGENT = b.OFFICE_NUM

left join air1.DJ_HIS_DATA_BIG_TEMP_NOW c
on TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') = c.DEP_DATE
	and concat(a.UP_LOCATION,a.DIS_LOCATION) = c.segment
	and SUBSTR(a.FLIGHT_NO, 3) = c.FLIGHT_NO

left join air1.AIR_PORT_DY d
on a.UP_LOCATION = d.AIR_PORT_CODE

left join air1.shifa_plf_his_benqi_EU e
on TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') = e.DEP_DATE
	and concat(a.UP_LOCATION,a.DIS_LOCATION) = e.SEGMENT
	and SUBSTR(a.FLIGHT_NO, 3)= e.FLIGHT_NO

left join air1.TB_FLIGHT_DETAIL f
on TO_CHAR(a.DEP_DATE, 'YYYY-MM-DD') = f.DEP_DATE
	and concat(a.UP_LOCATION,a.DIS_LOCATION) =  concat(f.UP_LOCATION,f.DIS_LOCATION)
	and SUBSTR(a.FLIGHT_NO, 3) = f.FLIGHT_NO

left join t1
on SUBSTR(a.FLIGHT_NO, 3) = t1.FLIGHT_NO
and concat(a.UP_LOCATION,a.DIS_LOCATION) =  t1.segment

where a.fop like  '%TP%'
-- and a.coupon_status in ('F','C','L')
and a.TK_DATE >= TRUNC(SYSDATE, 'YYYY')
and a.BOOKING_STATUS  in ('HK','KK','RR','RL','UN','TK')
order by a.TK_DATE



