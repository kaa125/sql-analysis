{{ config(schema='growth_data_mart') }}

WITH order_info AS 
(SELECT DISTINCT user_id, System_Delivery_Date 
FROM `dbt.all_gmv`
WHERE order_status_code IN (4,5))
,
user_orders AS (
  SELECT
    user_id, 
    System_Delivery_Date, 
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY System_Delivery_Date DESC) AS order_num
  FROM order_info
)
,
last_and_second_last_order_dates AS 
(SELECT 
user_id,
  MAX(CASE WHEN order_num = 1 THEN System_Delivery_Date END) AS last_order_date,
  MAX(CASE WHEN order_num = 2 THEN System_Delivery_Date END) AS second_last_order_date
FROM 
  user_orders
WHERE 
  order_num <= 2
GROUP BY 
  user_id
)
,
customer_performance_metrics AS 
(
  SELECT user_id, STRING_AGG(DISTINCT Category, ', ') AS categories,
  COUNT(DISTINCT CONCAT(user_id, System_Delivery_Date)) AS unique_user_orders,
  SAFE_DIVIDE(SUM(Delivered_GMV), COUNT(DISTINCT delivered_orders)) AS AOV,
  MIN(CASE WHEN order_status_code IN (4,5) THEN System_Delivery_Date END) AS first_order_date
  FROM `dbt.all_gmv`
  GROUP BY 1
)
,
nps_data AS 
(SELECT 
results.user_id AS user_id,
results.result_id AS result_id, results.created_at AS result_created_at, results.survey_id AS survey_id, surveys.survey_name AS survey_name,
results.question_id AS question_id ,surveys.question_type AS question_type, surveys.question_text AS question_text,
results.option_id AS option_id, surveys.option_text AS option_text,results.selected_option AS selected_option
FROM `data-warehouse.nps.results` results
LEFT JOIN `data-warehouse.nps.surveys` surveys
ON results.option_id = surveys.option_id AND results.question_id = surveys.question_id AND results.survey_id = surveys.survey_id
WHERE surveys.question_text LIKE '%??? ?? ????? ????????? ?? ?????? ???????%' OR surveys.question_text LIKE '%kya imkaan hai ke aap doosray dukandaron ko istemal karnay ka mashawara dein ge%')
,
most_recent_nps_score AS 
(SELECT *
FROM (
  SELECT
    *, TIMESTAMP_SECONDS(nps_data.result_created_at),
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY nps_data.result_created_at DESC) AS nps_survey
  FROM
    nps_data
)
WHERE
  nps_survey = 1 --AND user_id=74054
  ORDER BY user_id
)
,
cancellations_returns_last3months AS 
(SELECT user_id, 
SUM(Ordered_GMV) AS total_punched_GMV_last3months,
SUM(Delivered_GMV) AS total_NMV_last3months,
SUM(Dispatched_GMV) AS total_dispatched_GMV_last3months,
SUM(Return_GMV) AS total_return_GMV_last3months,
SUM( 
CASE WHEN order_status_code = 6 THEN Ordered_GMV
ELSE NULL
END
) AS total_cancelled_GMV_last3months
FROM `dbt.all_gmv`
WHERE System_Delivery_Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY 1
)
SELECT
retailers.user_id AS retailer_id,
users.id AS user_id,
users.name AS User_Name,
users.mobile,
retailers.store_name,
cities.name AS City,
areas.name AS Area,
sub_areas.name AS Sub_Area,
users.created_at + INTERVAL 5 HOUR AS signup_date,
users.is_active AS is_active,
store_type.name AS store_type,
customer_performance_metrics.categories,
customer_performance_metrics.first_order_date,
customer_performance_metrics.AOV,
customer_performance_metrics.unique_user_orders,
last_and_second_last_order_dates.last_order_date,
last_and_second_last_order_dates.second_last_order_date,
TIMESTAMP_SECONDS(most_recent_nps_score.result_created_at) AS result_created_at,
most_recent_nps_score.question_text,
most_recent_nps_score.selected_option,
cancellations_returns_last3months.total_punched_GMV_last3months,
cancellations_returns_last3months.total_NMV_last3months,
cancellations_returns_last3months.total_dispatched_GMV_last3months,
cancellations_returns_last3months.total_return_GMV_last3months,
cancellations_returns_last3months.total_cancelled_GMV_last3months
FROM  retailers
LEFT OUTER JOIN  users ON retailers.user_id = users.id
LEFT OUTER JOIN cities ON retailers.city_id = cities.id
LEFT OUTER JOIN areas ON areas.id = retailers.area_id
LEFT OUTER JOIN sub_areas ON sub_areas.id = retailers.sub_area_id
LEFT JOIN  store_type ON retailers.store_type = store_type.id
LEFT JOIN customer_performance_metrics ON customer_performance_metrics.user_id = retailers.user_id
LEFT JOIN last_and_second_last_order_dates ON last_and_second_last_order_dates.user_id = retailers.user_id
LEFT JOIN most_recent_nps_score ON most_recent_nps_score.user_id = retailers.user_id
LEFT JOIN cancellations_returns_last3months ON cancellations_returns_last3months.user_id = retailers.user_id
--WHERE most_recent_nps_score.selected_option IS NOT NULL
