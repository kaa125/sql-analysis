-- Define Orders and Payment Information
with
    orders as (select * from {{ ref("base_orders") }}),
    order_payments as (
        select *
        from
            (
                select
                    order_id,
                    case
                        when payment_types_id = 1 then 'COD'
                        when payment_types_id = 2 then 'Finja'
                        when payment_types_id = 3 then 'BNPL'
                        when payment_types_id = 4 then 'Wallet'
                    end as paymentmode,
                    total_discount,
                    amount,
                    return_amount
                from {{ ref("base_order_payments") }}
            ) 
            pivot (
                sum(coalesce(total_discount, 0)) as total_discount,
                sum(coalesce(amount, 0)) as amount,
                sum(coalesce(return_amount, 0)) as return_amount
                for paymentmode in ('COD', 'Finja', 'BNPL', 'Wallet')
            )
    ),
    -- Define Stops and Product Information
    stops as (select id, stop_status, route_id from {{ ref("base_stops") }}),
    product_sku as (select * from {{ ref("base_product_sku") }}),
    products as (select * from {{ ref("base_products") }}),
    product_sku_details as (select * from {{ ref("base_product_sku_details") }}),
    product_variants as (select * from {{ ref("base_product_variants") }}),
    users as (select * from {{ ref("base_users") }}),

    -- Retailer Table Scoring
    x as (
        select
            user_id,
            phone,
            city_id,
            store_name,
            latlng,
            area_id,
            sub_area_id,
            cnic_number,
            address,
            case when phone is not null then 1 else 0 end as s1,
            case when store_name is not null then 1 else 0 end as s2,
            case when latlng is not null then 1 else 0 end as s3,
            case when area_id is not null then 1 else 0 end as s4,
            case when sub_area_id is not null then 1 else 0 end as s5,
            case when cnic_number is not null then 1 else 0 end as s6,
            case when address is not null then 1 else 0 end as s7,
            case when city_id is not null then 1 else 0 end as s8
        from {{ ref("base_retailers") }}
    ),

    y as (
        select
            user_id,
            phone,
            store_name,
            city_id,
            latlng.lat as lat,
            latlng.lng as lng,
            area_id,
            sub_area_id,
            cnic_number,
            address,
            (s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8) as score,
            row_number() over (
                partition by user_id
                order by (s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8) desc
            ) as rank
        from x
    ),

    z as (select user_id, min(rank) as minrank from y group by 1),

    a as (select y.* from y left join z on y.user_id = z.user_id where rank = minrank),

    retailers as (select * from a),

    cities as (select * from {{ ref("base_cities") }}),
    suppliers as (select * from {{ ref("base_suppliers") }}),
    product_categories as (select * from {{ ref("base_product_categories") }}),
    categories as (select * from {{ ref("base_categories") }}),
    stop_status as (select * from {{ ref("base_stop_status") }}),
    routes as (select * from {{ ref("base_routes") }}),
    route_status as (select * from {{ ref("base_route_status") }}),
    orders_agents as (select * from {{ ref("base_orders_agents") }}),
    coupons as (select * from {{ ref("base_coupons") }}),
    verticals as (select * from {{ ref("base_verticals") }}),
    areas as (select * from {{ ref("base_areas") }}),
    sub_areas as (select * from {{ ref("base_sub_areas") }}),
    cash_collection as (select * from {{ ref("base_cash_collection") }}),
    order_items as (select distinct * except (id) from {{ ref("base_order_items") }}),

    -- Preprocess Transaction Logs Table
    p as (
        select stop_id, max(id) as maxid
        from {{ ref("base_transaction_logs") }}
        group by 1
    ),
    q as (select * from {{ ref("base_transaction_logs") }}),
    r as (select q.* from q left join p on q.stop_id = p.stop_id where p.maxid = q.id),
    transaction_logs as (select * from r),

    -- Calculate Order Metrics
    order_value as (
        select
            order_id,
            sum(order_items.price * order_items.qty) as `PunchedGMVForFakeFlag`,
            sum(
                order_items.price * (
                    case
                        when
                            orders.fulfilment_mode_id in (1, 2)
                            and orders.status in (4, 5)
                        then
                            (
                                coalesce(order_items.picked_qty, 0)
                                - coalesce(order_items.return_qty, 0)
                            )
                        when
                            orders.fulfilment_mode_id in (1, 2)
                            and stops.stop_status = 5
                        then 0
                        when
                            orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
                        then
                            (
                                coalesce(order_items.picked_qty, 0)
                                - coalesce(order_items.return_qty, 0)
                            )
                        else 0
                    end
                )
            ) as totalpriceperorder
        from order_items
        left join orders on order_items.order_id = orders.id
        left join stops on stops.id = orders.stop_id
        group by 1
    ),
    order_sku_value as (
        select
            order_id,
            sku_id,
            sum(
                order_items.price * (
                    case
                        when
                            orders.fulfilment_mode_id in (1, 2)
                            and orders.status in (4, 5)
                        then
                            (
                                coalesce(order_items.picked_qty, 0)
                                - coalesce(order_items.return_qty, 0)
                            )
                        when
                            orders.fulfilment_mode_id in (1, 2)
                            and stops.stop_status = 5
                        then 0
                        when
                            orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
                        then
                            (
                                coalesce(order_items.picked_qty, 0)
                                - coalesce(order_items.return_qty, 0)
                            )
                        else 0
                    end
                )
            ) as totalpriceperordersku
        from order_items
        left join orders on order_items.order_id = orders.id
        left join stops on stops.id = orders.stop_id
        group by 1, 2
    )

-- Main Query Starts Here
select distinct
    cities.name as city,

    -- Order Details
    orders.master_order_id as `MasterOrderID`,
    orders.id as `OrderID`,
    orders.is_fake as `is_fake`,
    case
        when orders.fulfilment_mode_id = 1
        then 'Fulfiled By Dastgyr'
        when orders.fulfilment_mode_id = 2
        then 'WaaS'
        when orders.fulfilment_mode_id = 3
        then 'Drop Shipping'
        else cast(orders.fulfilment_mode_id as string)
    end as `FulfilmentMode`,
    case
        when orders.vertical_id = 1
        then 'FMCG'
        when orders.vertical_id = 2
        then 'Constructíon'
        when orders.vertical_id = 3
        then 'Chemicals'
        else cast(orders.vertical_id as string)
    end as `Verticals`,
    timestamp_add(orders.created_at, interval 5 hour) as `OrderCreatedAt`,
    timestamp_add(orders.delivered_at, interval 5 hour) as `OrderDeliveredAt`,
    orders.user_id as `UserID`,
    timestamp_add(order_items.updated_at, interval 5 hour) as `updated_at`,
    case
        when orders.status = 0
        then 'Pending'
        when orders.status = 1
        then 'In Preparation'
        when orders.status = 2
        then 'Ready to Ship'
        when orders.status = 3
        then 'In Transit'
        when orders.status = 4
        then 'Delivered'
        when orders.status = 5
        then 'Closed'
        when orders.status = 6
        then 'Cancelled'
        when orders.status = 7
        then 'Return'
        when orders.status = 8
        then 'Shop Closed'
        when orders.status = 9
        then 'Waiting for approval'
        else cast(orders.status as string)
    end as `OrderStatus`,
    case
        when orders.type = 1
        then 'Regular Order'
        when orders.type = 2
        then 'Spot Sale'
        else cast(orders.type as string)
    end as `OrderType`,
    orders.stop_id as `StopID_Orders`,
    case
        when cities.is_exclusive = true or product_sku_details.user_id = 21347
        then true
        else false
    end as `PanPakistan`,

    -- SKU Details 
    products.name as `Product_Name`,
    products.id as `PRODUCT_ID`,
    product_variants.id as `Product Variant ID`,
    product_variants.name as `Product Variant Name`,
    categories.name as `SubCategory`,
    c2.name as `Category`,

    -- Price and Discounts
    order_items.price as `Price`,
    -- Special Discount, Promo Discount and Total Discount are at the order level;
    -- they are apportioned using order-SKU NMV / order NMV
    -- case when order_value.TotalPricePerOrder = 0 then 0 else
    -- (order_sku_value.TotalPricePerOrderSKU / order_value.TotalPricePerOrder) *
    -- COALESCE(orders.special_discount,0) end as `SpecialDiscount`,
    -- COALESCE(order_items.discount,0) as `BulkDiscount`,
    -- case when order_value.TotalPricePerOrder = 0 then 0 else
    -- (order_sku_value.TotalPricePerOrderSKU / order_value.TotalPricePerOrder) *
    -- COALESCE((COALESCE(order_payments.total_discount_COD,0)+
    -- COALESCE(order_payments.total_discount_Finja,0) +
    -- COALESCE(order_payments.total_discount_BNPL,0)+COALESCE(order_payments.total_discount_Wallet,0)),0) - COALESCE(order_items.discount,0) - (order_sku_value.TotalPricePerOrderSKU / order_value.TotalPricePerOrder) * COALESCE(orders.special_discount,0) end as `PromoDiscount`,
    -- case when order_value.TotalPricePerOrder = 0 then 0 else
    -- (order_sku_value.TotalPricePerOrderSKU / order_value.TotalPricePerOrder) *
    -- COALESCE((COALESCE(order_payments.total_discount_COD,0)+
    -- COALESCE(order_payments.total_discount_Finja,0) +
    -- COALESCE(order_payments.total_discount_BNPL,0)+COALESCE(order_payments.total_discount_Wallet,0)),0) end as `TotalDiscount`,
    order_items.commission_value__fl as `CommissionPercentage`,
    (
        order_items.commission_value__fl
        / 100
        * (
            case
                when orders.fulfilment_mode_id in (1, 2) and orders.status in (4, 5)
                then
                    (
                        coalesce(order_items.picked_qty, 0)
                        - coalesce(order_items.return_qty, 0)
                    )
                when orders.fulfilment_mode_id in (1, 2) and stops.stop_status = 5
                then 0
                when orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
                then
                    (coalesce(order_items.qty, 0) - coalesce(order_items.return_qty, 0))
                else 0
            end
        )
        * order_items.price
    ) as `ActualizedCommissions`,

    -- Retailer Details
    orders.user_id as `CustomerUserID`,
    retailers.phone as `RetailerPhoneNumber`,
    retailers.store_name as `RetailerName`,
    retailers.lat as `RetailerLocationLat`,
    retailers.lng as `RetailerLocationLng`,
    retailers.area_id as `AreaID`,
    retailers.sub_area_id as `SubAreaID`,
    retailers.cnic_number as `RetailerCNIC_Number`,
    retailers.address as `RetailerAddress`,

    -- Agent Details
    signup_agents.name as `SignupAgentName`,
    orders_agents.agent_id as `AgentID`,
    order_agents.name as `OrderAgentName`,
    drivers.name as `DriverName`,
    drivers.mobile as `DriverPhoneNumber`,

    -- Seller Details
    product_sku_details.user_id as `SellerID`,
    sellers.name as `SellerName`,
    sellers.mobile as `SellerPhoneNumber`,
    areas.name as `Area`,
    sub_areas.name as `SubArea`,
    case
        when suppliers.is_internal = true
        then 'Inventory-Led'
        when suppliers.is_internal = false
        then 'Marketplace'
        else 'Other'
    end as `SupplierType`,

    -- Route and Stop Details
    routes.id as `RouteID`,
    routes.name as `RouteName`,
    routes.created_at as `RouteCreatedAt`,
    routes.started_at as `RouteStartedAt`,
    cash_collection.created_at as `RouteEndTime`,
    route_status.name as `RouteStatus`,
    stops.id as `StopID_Stops`,
    stop_status.name as `StopStatus`,

    timestamp_add(transaction_logs.created_at, interval 5 hour) as `PaymentTime`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.amount_cod, 0)
        else 0
    end as `OrderPaymentAmountCOD`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.amount_finja, 0)
        else 0
    end as `OrderPaymentAmountFinja`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.amount_bnpl, 0)
        else 0
    end as `OrderPaymentAmountBNPL`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.amount_wallet, 0)
        else 0
    end as `OrderPaymentAmountWallet`,

    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.return_amount_cod, 0)
        else 0
    end as `ReturnAmountCOD`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.return_amount_finja, 0)
        else 0
    end as `ReturnAmountFinja`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.return_amount_bnpl, 0)
        else 0
    end as `ReturnAmountBNPL`,
    case
        when order_value.totalpriceperorder != 0
        then
            (order_sku_value.totalpriceperordersku / order_value.totalpriceperorder)
            * coalesce(order_payments.return_amount_wallet, 0)
        else 0
    end as `ReturnAmountWallet`,

    -- Order Metrics (Quantities)
    case
        when orders.type = 1 then coalesce(order_items.qty, 0) else 0
    end as `QtyOrdered`,
    -- Since drop shipped orders don't go through our usual ops flow, picked,
    -- dispatched and spot sold qty metrics do not apply to DS orders
    case
        when orders.fulfilment_mode_id in (1, 2)
        then coalesce(order_items.picked_qty, 0)
        else null
    end as `PickedQty`,
    case
        when orders.fulfilment_mode_id in (3)
        then null
        when orders.type in (1) and orders.fulfilment_mode_id in (1, 2)
        then coalesce(order_items.picked_qty, 0)
        else 0
    end as `DispatchedQty`,
    case
        when orders.fulfilment_mode_id in (3)
        then null
        when orders.type in (2) and orders.fulfilment_mode_id in (1, 2)
        then coalesce(order_items.picked_qty, 0)
        else 0
    end as `SpotSoldQty`,
    case
        when orders.fulfilment_mode_id in (1, 2) and orders.status in (4, 5)
        then (coalesce(order_items.picked_qty, 0) - coalesce(order_items.return_qty, 0))
        when orders.fulfilment_mode_id in (1, 2) and stops.stop_status = 5
        then 0
        when orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
        then (coalesce(order_items.qty, 0) - coalesce(order_items.return_qty, 0))
        else 0
    end as `DeliveredQty`,
    (
        case
            when orders.fulfilment_mode_id in (1, 2) and orders.status in (4, 5)
            then coalesce(order_items.return_qty, 0)
            when orders.fulfilment_mode_id in (1, 2) and stops.stop_status = 5
            then coalesce(order_items.picked_qty, 0)
            when orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
            then coalesce(order_items.return_qty, 0)
            else 0
        end
    ) as `ReturnQty`,
    (
        case when orders.status = 6 then coalesce(order_items.qty, 0) else 0 end
    ) as `CancelledQty`,
    (
        case
            when
                orders.fulfilment_mode_id in (1, 2)
                and orders.status = 6
                and stops.id is null
            then coalesce(order_items.qty, 0)
            when orders.fulfilment_mode_id in (3)
            then null
            else 0
        end
    ) as `PreDispatchCancelledQty`,
    order_items.qty as `OrderItemsQty`,
    -- Order Metrics (Revenue)
    (case when orders.type = 1 then coalesce(order_items.qty, 0) else 0 end)
    * order_items.price as `TotalPunchedGMV`,
    -- (CASE WHEN orders.fulfilment_mode_id IN (1, 2) THEN coalesce(order_items.picked_qty, 0) WHEN orders.fulfilment_mode_id IN (3) THEN NULL END) * order_items.price AS `TotalPickedGMV`,
    -- (CASE WHEN orders.type IN (1) AND orders.fulfilment_mode_id IN (1, 2) THEN coalesce(order_items.picked_qty, 0) ELSE 0 END) * order_items.price AS `TotalDispatchedGMV`,
    -- (CASE WHEN orders.type IN (2) AND orders.fulfilment_mode_id IN (1, 2) THEN coalesce(order_items.picked_qty, 0) ELSE 0 END) * order_items.price AS `TotalSpotSoldGMV`,
    (case
        when orders.fulfilment_mode_id in (1, 2) and orders.status in (4, 5)
        then (coalesce(order_items.picked_qty, 0) - coalesce(order_items.return_qty, 0))
        when orders.fulfilment_mode_id in (1, 2) and stops.stop_status = 5
        then 0
        when orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
        then (coalesce(order_items.qty, 0) - coalesce(order_items.return_qty, 0))
        else 0
    end)
    * order_items.price as `TotalDeliveredGMV`,
    (case
        when orders.fulfilment_mode_id in (1, 2) and orders.status = 6
        then coalesce(order_items.qty, 0)
        when orders.fulfilment_mode_id in (3)
        then null
        else 0
    end)
    * order_items.price as `TotalCancelledGMV`,
    (case when orders.status = 6 then coalesce(order_items.qty, 0) else 0 end)
    * order_items.price as `TotalPreDispatchCancelledGMV`,
    case
        when orders.fulfilment_mode_id in (1, 2) and orders.status in (4, 5)
        then coalesce(order_items.picked_qty, 0) - coalesce(order_items.return_qty, 0)
        when orders.fulfilment_mode_id in (1, 2) and stops.stop_status = 5
        then 0
        when orders.fulfilment_mode_id in (3) and orders.status in (4, 5)
        then coalesce(order_items.qty, 0) - coalesce(order_items.return_qty, 0)
        else 0
    end as `TotalQty`,
    case when orders.status = 6 then coalesce(order_items.qty, 0) else 0 end as `TotalCancelledQty`,
    (case when orders.fulfilment_mode_id in (1, 2) and orders.status = 6 then coalesce(order_items.qty, 0) else 0 end) as `TotalPreDispatchCancelledQty`,
    -- Order Metrics (SKUs)
    order_sku_value.totalpriceperordersku as `TotalPricePerOrderSKU`,
    (case when orders.status = 6 then order_sku_value.totalpriceperordersku else 0 end) as `TotalPricePerCancelledOrderSKU`,
    case when orders.status = 6 then order_items.price else 0 end as `TotalCancelledOrderSKUPrice`,

    -- Transaction Details
    transaction_logs.id as `TransactionID`,
    case when order_payments.paymentmode = 'COD' then coalesce(order_payments.amount_cod, 0) else 0 end as `TransactionAmountCOD`,
    case when order_payments.paymentmode = 'Finja' then coalesce(order_payments.amount_finja, 0) else 0 end as `TransactionAmountFinja`,
    case when order_payments.paymentmode = 'BNPL' then coalesce(order_payments.amount_bnpl, 0) else 0 end as `TransactionAmountBNPL`,
    case when order_payments.paymentmode = 'Wallet' then coalesce(order_payments.amount_wallet, 0) else 0 end as `TransactionAmountWallet`,

    case when order_payments.paymentmode = 'COD' then coalesce(order_payments.return_amount_cod, 0) else 0 end as `ReturnTransactionAmountCOD`,
    case when order_payments.paymentmode = 'Finja' then coalesce(order_payments.return_amount_finja, 0) else 0 end as `ReturnTransactionAmountFinja`,
    case when order_payments.paymentmode = 'BNPL' then coalesce(order_payments.return_amount_bnpl, 0) else 0 end as `ReturnTransactionAmountBNPL`,
    case when order_payments.paymentmode = 'Wallet' then coalesce(order_payments.return_amount_wallet, 0) else 0 end as `ReturnTransactionAmountWallet`
from orders
left join order_items on orders.id = order_items.order_id
left join products on order_items.product_id = products.id
left join product_variants on order_items.variant_id = product_variants.id
left join product_sku_details on product_variants.product_sku_id = product_sku_details.id
left join product_categories on product_sku_details.product_category_id = product_categories.id
left join categories on product_categories.category_id = categories.id
left join cities on orders.city_id = cities.id
left join sellers on product_sku_details.user_id = sellers.id
left join suppliers on sellers.supplier_id = suppliers.id
left join routes on orders.route_id = routes.id
left join route_status on routes.route_status_id = route_status.id
left join stops on orders.stop_id = stops.id
left join stop_status on stops.stop_status = stop_status.id
left join orders_agents on orders.id = orders_agents.order_id
left join agents as signup_agents on orders.signup_agent_id = signup_agents.id
left join agents as order_agents on orders_agents.agent_id = order_agents.id
left join drivers on orders.driver_id = drivers.id
left join transaction_logs on orders.transaction_log_id = transaction_logs.id
left join cash_collection on routes.id = cash_collection.route_id
left join order_payments on orders.id = order_payments.order_id
left join order_value on orders.id = order_value.order_id
left join order_sku_value on orders.id = order_sku_value.order_id
left join areas on retailers.area_id = areas.id
left join sub_areas on retailers.sub_area_id = sub_areas.id
where
    orders.created_at >= '2023-01-01'
    and orders.created_at < '2023-01-31'
    and (orders.fulfilment_mode_id in (1, 2, 3) or stops.stop_status = 5)
order by `MasterOrderID`, `OrderID`, `OrderType`, `OrderCreatedAt`, `Product_Name`, `Product Variant Name`;
