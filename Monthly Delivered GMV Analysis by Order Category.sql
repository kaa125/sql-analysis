with a as (
With x AS
(
SELECT cities.name AS "City",orders.master_order_id, orders.id AS order_id, orders.created_at + interval '5Hr' AS "Order Time", CAST(orders.delivered_at + interval '5Hr' AS DATE) AS "System Delivery Date", CAST(orders.delivery_date_time + INTERVAL '5Hr' AS DATE) AS "Actual Delivery Date", orders.delivery_date_time + INTERVAL '5Hr' AS "Actual Delivery Time",
                    orders.status as order_status_code,
                    CASE WHEN orders.status = 0 then 'Pending' when orders.status = 2 then 'Ready To Ship' when orders.status = 3 then 'Shipped' WHEN orders.status = 4 then 'Delivered' WHEN orders.status = 5 THEN 'Closed' WHEN orders.status = 6 THEN 'Cancelled' ELSE '' END AS order_status_text,
                    cancellation_reasons.description AS "Cancellation Reason",
                    order_items.sku_id, products.name AS "SKU", order_items.price AS "Order Price",
                     c2.name AS "Category",categories.name AS "Sub-Category",
                    sellers.id AS seller_id, sellers.name AS "Seller",
                    CASE WHEN orders.type = 1 THEN order_items.qty ELSE 0 END AS "Qty Ordered",
                    CASE WHEN orders.status IN (4,5) THEN COALESCE(order_items.qty, 0) - COALESCE(order_items.return_qty, 0) ELSE 0 END AS "Delivered Qty",
                    (CASE WHEN orders.status IN (6) THEN COALESCE(order_items.qty, 0) ELSE 0 END)  AS "Cancelled Qty",
                    CASE WHEN orders.type = 1 THEN order_items.price * order_items.qty ELSE 0 END AS "Ordered GMV",
                    CASE WHEN orders.status IN (4,5) THEN (COALESCE(order_items.qty, 0) - COALESCE(order_items.return_qty, 0)) * order_items.price ELSE 0 END AS "Delivered GMV",
                    order_items.commission_value AS "Commission Rate (%)",((CASE WHEN orders.status IN (4,5) THEN (COALESCE(order_items.qty, 0) - COALESCE(order_items.return_qty, 0)) * order_items.price ELSE 0 END)*( order_items.commission_value/100::FLOAT)) AS "Commission Value",
                    (CASE WHEN orders.status IN (6) THEN COALESCE(order_items.qty, 0) ELSE 0 END) * order_items.price AS "Cancelled GMV",
                    orders.user_id, users.name AS "User Name", users.mobile, retailers.store_name, retailers.address, retailers.latlng,
                    areas.name AS "Area", sub_areas.name AS "Sub-Area",
                    order_agents.id AS order_agent_id, order_agents.name AS "Order Agent", signup_agents.id AS signup_agent_id, signup_agents.name AS "Signup Agent", CASE WHEN orders.created_by IS NULL THEN 'Retailer App' ELSE 'Sales Portal' END AS "Order Channel"
            FROM orders
            LEFT JOIN order_items ON orders.id = order_items.order_id
            LEFT JOIN product_sku ON order_items.sku_id = product_sku.id
            LEFT JOIN products ON product_sku.product_id = products.id
            LEFT JOIN product_categories ON products.id = product_categories.product_id
            LEFT JOIN categories ON product_categories.category_id = categories.id
            LEFT JOIN categories AS c2 ON c2.id = categories.parent_id
            LEFT JOIN retailers on orders.user_id = retailers.user_id
            LEFT JOIN users ON retailers.user_id = users.id
            LEFT JOIN cities ON retailers.city_id = cities.id
            LEFT JOIN areas ON areas.id = retailers.area_id
            LEFT JOIN sub_areas ON sub_areas.id = retailers.sub_area_id
            LEFT JOIN stops ON stops.id = orders.stop_id
            LEFT JOIN stop_status ON stop_status.id = stops.stop_status
            LEFT JOIN routes ON routes.id = stops.route_id
            LEFT JOIN route_status ON route_status.id = routes.status
            LEFT JOIN users AS drivers ON drivers.id = routes.user_id
            LEFT JOIN transaction_logs ON transaction_logs.stop_id = orders.stop_id
            LEFT JOIN cash_collection ON cash_collection.route_id = routes.id
            LEFT JOIN orders_agents ON orders_agents.order_id = orders.id::TEXT
            LEFT JOIN users AS order_agents ON order_agents.id::TEXT = orders_agents.agent_id
            LEFT JOIN users AS signup_agents ON signup_agents.id = users.refer_by
            LEFT JOIN product_sku_details ON product_sku_details.sku_id = order_items.sku_id
            LEFT JOIN users AS sellers ON sellers.id = product_sku_details.user_id
            left join cancellation_reasons ON orders.cancel_reason_id = cancellation_reasons.id
            WHERE
                    orders.delivered_at IS NOT NULL
                    AND orders.fulfilment_mode_id = 3
                    AND orders.is_fake = false
            ORDER BY "City", "System Delivery Date" DESC, "Order Time" DESC
    )
    Select DISTINCT x.*, CASE WHEN x."Ordered GMV" < 50000 THEN 'RDS' ELSE 'BULK' END AS "RDS/BULK Classification" from x
    left join categories on categories.name = x."Category"
    where categories.name is not null
    ORDER BY "City", "System Delivery Date" DESC, "Order Time" desc
  )
SELECT extract(month from "System Delivery Date") as month_name,SUM("Delivered GMV") 
FROM a 
GROUP BY 1