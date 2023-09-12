select DISTINCT cities.name as "City",
        c2.id as "Category ID", c2.name AS "Category",
       categories.id as "Sub-Category ID", categories.name as "Sub-Category",
       c3.id AS "Sub Sub-Category ID", c3.name AS "Sub Sub-Category",
       CASE WHEN fulfilment_mode_id = 1 then 'Fulfiled By Dastgyr' WHEN fulfilment_mode_id = 2 THEN 'WaaS' WHEN fulfilment_mode_id = 3 THEN 'Drop Shipping' ELSE NULL END AS "Fulfilment Mode",
       commission_value as "Commission Rate (%)",
       categories_commission.is_active as "Is Active",
       u1.name as "Created By",
       u2.name as "Updated By",
       categories_commission.created_at + interval '5Hr' as "Created At",
       categories_commission.updated_at + interval '5Hr' as "Updated At",
       categories_commission.deleted_at + interval '5Hr' as "Deleted At"
from categories_commission
left join categories
on categories_commission.category_id = categories.id
left join categories c2 ON categories.parent_id = c2.id
left join categories c3 ON c3.parent_id = categories.id
left join cities
on cities.id = categories_commission.city_id
left join users u1
on u1.id = categories_commission.created_by
left join users u2
on u2.id = categories_commission.updated_by
left join fulfilment_mode on fulfilment_mode.id = categories_commission.fulfilment_mode_id
where categories_commission.id IS NOT NULL
order by 1, 2,3, 4