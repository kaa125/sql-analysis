{{ config(schema='growth_data_mart') }}

with all_quarter_dates1 as (
SELECT 
  CONCAT(EXTRACT(YEAR FROM first_day_of_quarter), '-Q', CAST(EXTRACT(QUARTER FROM first_day_of_quarter) AS STRING)) AS quarter, 
  first_day_of_quarter
FROM 
  UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS first_day_of_quarter
WHERE 
  EXTRACT(DAY FROM first_day_of_quarter) = 1 AND EXTRACT(MONTH FROM first_day_of_quarter) IN (1, 4, 7, 10)
)
,
all_quarter_dates as (
  select *, DATE_SUB(DATE_ADD(DATE_TRUNC(DATE(first_day_of_quarter), QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY) AS last_day_of_quarter 
  from all_quarter_dates1
)
,
rfm1 as 
(
  select 
  user_id,
--  System_Delivery_Date,
  DATE_TRUNC(System_Delivery_Date, QUARTER) AS quarter_from_system_delivery_date,
  --CONCAT('Q', CAST(EXTRACT(QUARTER FROM System_Delivery_Date) AS STRING), ' ', CAST(EXTRACT(YEAR FROM System_Delivery_Date) AS STRING)) AS quarter2,
  sum(Delivered_GMV) as NMV, COUNT(Distinct delivered_orders) as delivered_orders,
  SAFE_DIVIDE(sum(Delivered_GMV),(COUNT(Distinct delivered_orders))) as AOV,
  count(distinct CONCAT(CAST(EXTRACT(month FROM System_Delivery_Date) AS STRING), ' ', CAST(EXTRACT(YEAR FROM System_Delivery_Date) AS STRING))) as months_of_orders_all_status,
  COUNT(Distinct concat(user_id, System_Delivery_Date)) as unique_user_orders,
  max(system_delivery_date) as last_order_date,
  min(system_delivery_date) as first_order_date,

from `dbt_dastgyr.all_gmv`
where 
--order_status_code in (4,5) and 
--user_id = 8880 and 
  system_delivery_date >= '2022-01-01'
group by 1,2
)
,
result1 as 
(select distinct all_quarter_dates.*,
rfm1.user_id as rfm_user_id
from all_quarter_dates 
cross join rfm1 
)
,
result2 as 
(
   select distinct * from result1 left join rfm1 on result1.rfm_user_id = rfm1.user_id and result1.first_day_of_quarter = rfm1.quarter_from_system_delivery_date
)
,
rfm2 as 
(
select tab.last_day_of_quarter , tab.first_day_of_quarter, tab.rfm_user_id, ag.user_id as rfm2_user_id,
sum(CASE 
            WHEN tab.last_day_of_quarter >= ag.system_delivery_date THEN ag.Delivered_GMV
            ELSE 0 
        END) AS Cumulative_NMV
,
count(distinct CASE 
            WHEN tab.last_day_of_quarter >= ag.system_delivery_date THEN ag.delivered_orders
            ELSE NULL 
        END) AS Cumulative_delivered_orders

,
count(distinct CASE 
            WHEN tab.last_day_of_quarter >= ag.system_delivery_date THEN concat(ag.user_id, ag.System_Delivery_Date)
            ELSE NULL 
        END) AS Cumulative_unique_user_orders

,
max(CASE 
            WHEN tab.last_day_of_quarter >= ag.system_delivery_date THEN ag.system_delivery_date
            ELSE NULL 
        END) AS Cumulative_last_order_date


,
min(CASE 
            WHEN tab.last_day_of_quarter >= ag.system_delivery_date THEN ag.system_delivery_date
            ELSE NULL 
        END) AS Cumulative_first_order_date

from result2 tab left join `dbt_dastgyr.all_gmv` ag on tab.rfm_user_id = ag.user_id
where ag.system_delivery_date >= '2022-01-01'
--where tab.last_day_of_quarter = '2023-03-31'
group by 1,2,3,4
)

select * from rfm2 
left join rfm1 
on rfm1.user_id = rfm2.rfm2_user_id and rfm1.quarter_from_system_delivery_date = rfm2.first_day_of_quarter