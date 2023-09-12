with returns_discrepancy1 AS
            (
            WITH x AS
                        (
                        SELECT cities.name AS "City", return_discrepancy.created_at + interval '5Hr' AS "Time", 
                                return_discrepancy.route_id, routes.name AS "Route",
                                return_discrepancy.sku_id, products.name AS "SKU", 
                                return_discrepancy.qty AS "App Return Qty", COALESCE(return_discrepancy.discrepancy,0) AS "Discrepancy in Return Qty", return_discrepancy.qty - return_discrepancy.discrepancy AS "Actual Return Qty",
                                reasons.reason AS "Discrepancy Reason",
                                route_status.name AS "Route Status", drivers.user_id AS driver_id, users.name AS "Driver"
                        FROM return_discrepancy
                        LEFT JOIN reasons ON reasons.id = return_discrepancy.reason_id
                        LEFT JOIN product_sku ON product_sku.id = return_discrepancy.sku_id
                        LEFT JOIN products ON product_sku.product_id = products.id
                        INNER JOIN routes ON routes.id = return_discrepancy.route_id
                        LEFT JOIN route_status ON route_status.id = routes.status
                        INNER JOIN users ON users.id = routes.user_id
                        LEFT JOIN drivers ON drivers.user_id = users.id
                        LEFT JOIN cities ON cities.id = drivers.city_id
                        WHERE return_discrepancy.id IS NOT NULL
                                AND return_discrepancy.reason_id IS NOT NULL
                        )
                        
            SELECT "City", CAST("Time" AS DATE) AS "Date", route_id, "Route", sku_id, "SKU", 
                    AVG("App Return Qty") +
                        (
                        SUM(CASE WHEN "Discrepancy Reason" = 'Wastage' THEN "Discrepancy in Return Qty" ELSE 0 END) +
                        SUM(CASE WHEN "Discrepancy Reason" = 'Lost/Stolen' THEN "Discrepancy in Return Qty" ELSE 0 END) +
                        SUM(CASE WHEN "Discrepancy Reason" = 'Wrong data entry' THEN "Discrepancy in Return Qty" ELSE 0 END)
                        ) AS "App Return Qty",
                    SUM(CASE WHEN "Discrepancy Reason" = 'Wastage' THEN "Discrepancy in Return Qty" ELSE 0 END) AS "Discrepancy Due to Wastage",
                    SUM(CASE WHEN "Discrepancy Reason" = 'Lost/Stolen' THEN "Discrepancy in Return Qty" ELSE 0 END) AS "Discrepancy Due to Lost/Stolen",
                    SUM(CASE WHEN "Discrepancy Reason" = 'Wrong data entry' THEN "Discrepancy in Return Qty" ELSE 0 END) AS "Discrepancy Due to Wrong data entry",
                    AVG("App Return Qty") AS "Actual Return Qty", 
                    "Route Status", driver_id, "Driver"
            FROM x
            GROUP BY "City", "Date", route_id, "Route", sku_id, "SKU", "Route Status", driver_id, "Driver"
            ORDER BY "City", "Date", route_id, sku_id
            )
            ,
            x AS
            (
            SELECT cities.name AS "City", orders.id AS order_id, 
                    orders.created_at + interval '5Hr' AS "Order Date", CAST(orders.delivered_at + interval '5Hr' AS DATE) AS "Delivery Date", 
                    orders.status AS order_status, stops.stop_status, stop_status.name AS "Stop Status",
                    order_items.seller_status as "Seller Order Status",
                    order_items.sku_id, products.name AS "SKU", order_items.price AS "Order Price", (CASE WHEN orders.status in (4,5) THEN order_items.discount ELSE 0 END) AS "Bulk Discount",
                    categories.id as "Sub-Category ID", categories.name AS "Sub-Category", 
                    c2.id as "Category ID", c2.name AS "Category",
                    sellers.id AS seller_id, sellers.name AS "Seller", seller_invoice_orders.invoice_id,
                    CASE WHEN orders.fulfilment_mode_id = 1 THEN 'Fulfilled By Dastgyr' WHEN orders.fulfilment_mode_id = 2 THEN 'WaaS' WHEN orders.fulfilment_mode_id = 3 THEN 'Drop-Shipping' ELSE orders.fulfilment_mode_id::TEXT END as "Fulfilment Mode",
                    CASE WHEN orders.stop_ID IS NOT NULL AND orders.type = 1 THEN order_items.qty ELSE 0 END AS "Qty Ordered", 
                    CASE WHEN orders.type in (1) THEN COALESCE(order_items.picked_qty, 0) ELSE 0 END AS "Dispatched Qty",
                    CASE WHEN orders.type in (2) THEN COALESCE(order_items.picked_qty, 0) ELSE 0 END AS "SS Qty",
                    CASE WHEN orders.fulfilment_mode_id in (1,2) AND orders.status in (4,5) THEN (COALESCE(order_items.picked_qty, 0) - COALESCE(order_items.return_qty, 0)) WHEN orders.fulfilment_mode_id in (3) AND orders.status in (4,5) THEN (COALESCE(order_items.qty, 0) - COALESCE(order_items.return_qty, 0)) ELSE 0 END AS "Delivered Qty",
                    (CASE WHEN stops.stop_status = 4 THEN COALESCE(order_items.return_qty, 0) WHEN stops.stop_status = 5 THEN COALESCE(order_items.picked_qty, 0) ELSE 0 END) - (CASE WHEN orders.type = 2 THEN order_items.picked_qty ELSE 0 END) AS "Return Qty",
                    CASE WHEN orders.type = 1 THEN (order_items.price * order_items.qty) ELSE 0 END AS "Total Punched GMV",
                    CASE WHEN orders.stop_ID IS NOT NULL AND orders.type = 1 THEN order_items.price * order_items.qty ELSE 0 END AS "Ordered GMV",
                    CASE WHEN orders.type in (1) THEN (order_items.price * coalesce(order_items.picked_qty,0)) ELSE 0 END AS "Dispatched GMV",--order_type = 1 ??
                    (CASE WHEN orders.fulfilment_mode_id in (1,2) AND orders.status in (4,5) THEN (COALESCE(order_items.picked_qty, 0) - COALESCE(order_items.return_qty, 0)) WHEN orders.fulfilment_mode_id in (3) AND orders.status in (4,5) THEN (COALESCE(order_items.qty, 0) - COALESCE(order_items.return_qty, 0)) ELSE 0 END)*order_items.price AS "Delivered GMV",
                    order_items.commission_value as "Commission Rate (%)",
                    ((CASE WHEN stops.stop_status = 4 THEN COALESCE(order_items.return_qty, 0) WHEN stops.stop_status = 5 THEN COALESCE(order_items.picked_qty, 0) ELSE 0 END) * order_items.price) - (CASE WHEN orders.type = 2 THEN order_items.picked_qty*order_items.price ELSE 0 END) AS "Return GMV",
                    transaction_logs.created_at + interval '5Hr' AS "Payment/Delivery Time",
                    orders.user_id, users.name AS "User Name", users.mobile, retailers.store_name, retailers.address, retailers.latlng,
                    areas.name AS "Area", sub_areas.name AS "Sub-Area",
                    orders.stop_id, routes.id AS route_id, routes.name AS "Route", route_status.name AS "Route Status", 
                    routes.started_at + interval '5Hr' AS "Route Start Time", cash_collection.created_at + interval '5Hr' AS "Route End Time", 
                    drivers.id AS driver_id, drivers.name AS "Driver",
                    order_agents.id AS order_agent_id, order_agents.name AS "Order Agent", signup_agents.id AS signup_agent_id, signup_agents.name AS "Signup Agent"
            FROM orders
            LEFT JOIN order_items ON orders.id = order_items.order_id
            LEFT JOIN seller_invoice_orders ON orders.id = seller_invoice_orders.order_id
            LEFT JOIN product_sku ON order_items.sku_id = product_sku.id
            LEFT JOIN products ON product_sku.product_id = products.id
            LEFT JOIN product_categories ON products.id = product_categories.product_id
            LEFT JOIN categories ON product_categories.category_id = categories.id
            LEFT JOIN categories AS c2 ON c2.id = categories.parent_id
            LEFT JOIN retailers on orders.user_id = retailers.user_id
            LEFT JOIN users ON retailers.user_id = users.id
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
            LEFT JOIN suppliers ON product_sku_details.user_id = suppliers.user_id
            LEFT JOIN cities ON suppliers.city_id = cities.id
            where orders.delivered_at IS NOT NULL
                    AND orders.is_fake = FALSE
                    AND orders.fulfilment_mode_id in (1,2)
            ) 
            
            ,y as (
SELECT x."City", "Delivery Date", 
        x."Seller Order Status", 
        x.route_id, x."Route", "Route End Time", x."Route Status", x.driver_id, x."Driver", stop_id, user_id retailer_user_id,"User Name" as "Retailer Name", mobile AS "Retailer Mobile", order_id, x."Fulfilment Mode",
        x.sku_id, x."SKU", 
        "Category ID", "Category", "Sub-Category ID", "Sub-Category",
        seller_id, "Seller", x.invoice_id AS "Seller Invoice ID", "Commission Rate (%)",
        SUM("Qty Ordered") AS "Qty Ordered", SUM("Dispatched Qty") AS "Dispatched Qty", SUM("Delivered Qty") AS "Delivered Qty",SUM("Return Qty") AS "Return Qty", --SUM("Discrepancy in Return Qty") AS "Discrepancy in Return Qty",
        SUM("SS Qty") AS "SS Qty",
        SUM("Total Punched GMV") AS "Total Punched GMV",SUM("Total Punched GMV"-"Ordered GMV") AS "Pre-Dispatch Cancellation" ,SUM("Ordered GMV") AS "Ordered GMV", SUM("Ordered GMV"-"Dispatched GMV") AS "Unfulfilled",
        SUM("Dispatched GMV") AS "Dispatched GMV", SUM("Delivered GMV") AS "Delivered GMV", SUM("Bulk Discount") AS "Bulk Discount",
        ROUND(SUM("Delivered GMV") * "Commission Rate (%)"/100) AS "Commission Value",
        SUM("Return GMV") AS "Return GMV",
        CASE WHEN SUM("Ordered GMV") != 0 THEN SUM("Dispatched GMV")/SUM("Ordered GMV") ELSE 0 END AS "Fulfilled (Dispatched) GMV %", 
        CASE WHEN SUM("Dispatched GMV") != 0 THEN SUM("Delivered GMV")/SUM("Dispatched GMV")::FLOAT ELSE 0 END AS "D2N %",
        CASE WHEN SUM("Dispatched GMV") != 0 THEN SUM("Return GMV")/SUM("Dispatched GMV")::FLOAT ELSE 0 END AS "D2R %"
FROM x
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)

Select y.*, y."Delivered GMV" - y."Bulk Discount" AS "Delivered Value (Net of Bulk Discounts)", y."Return Qty" - COALESCE(returns_discrepancy1."Actual Return Qty", y."Return Qty") AS "Discrepency in Returns(To be Charged)" 
from y
LEFT JOIN returns_discrepancy1 ON returns_discrepancy1.route_id = y.route_id AND returns_discrepancy1.sku_id = y.sku_id
ORDER BY "City", "Delivery Date" DESC, route_id,sku_id
limit 500