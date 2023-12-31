select * FROM(
select changes -> 'current' ->> 'id' AS id,
  changes -> 'current' ->> 'mrp' AS mrp,
  changes -> 'current' ->> 'code' AS code,
  changes -> 'current' ->> 'rank' AS rank,
  changes -> 'current' ->> 'price' AS price,
  changes -> 'current' ->> 'is_hot' AS is_hot,
  changes -> 'current' ->> 'weight' AS weight,
  changes -> 'current' ->> 'barcode' AS barcode,
  changes -> 'current' ->> 'is_aoos' AS is_aoos,
  changes -> 'current' ->> 'is_bulk' AS is_bulk,
  changes -> 'current' ->> 'is_deal' AS is_deal,
  changes -> 'current' ->> 'discount' AS discount,
  changes -> 'current' ->> 'is_stock' AS is_stock,
  changes -> 'current' ->> 'cities_id' AS cities_id,
  changes -> 'current' ->> 'is_active' AS is_active,
  changes -> 'current' ->> 'max_limit' AS max_limit,
  changes -> 'current' ->> 'min_limit' AS min_limit,
  changes -> 'current' ->> 'aoos_limit' AS aoos_limit,
  changes -> 'current' ->> 'best_price' AS best_price,
  changes -> 'current' ->> 'created_at' AS created_at,
  changes -> 'current' ->> 'deleted_at' AS deleted_at,
  changes -> 'current' ->> 'deleted_by' AS deleted_by,
  changes -> 'current' ->> 'is_deleted' AS is_deleted,
  changes -> 'current' ->> 'is_visible' AS is_visible,
  changes -> 'current' ->> 'product_id' AS product_id,
  changes -> 'current' ->> 'updated_at' AS updated_at,
  changes -> 'current' ->> 'updated_by' AS updated_by,
  changes -> 'current' ->> 'is_new_label' AS is_new_label,
  changes -> 'current' ->> 'back_in_stock' AS back_in_stock,
  changes -> 'current' ->> 'delivery_date' AS delivery_date,
  changes -> 'current' ->> 'current_aoos_inv' AS current_aoos_inv,
  changes -> 'current' ->> 'fulfilment_mode_id' AS fulfilment_mode_id,
  changes -> 'current' ->> 'product_variant_id' AS product_variant_id,
  changes -> 'sku' ->> 'id' AS sku_id,
  changes -> 'sku' ->> 'price' AS sku_price,
  changes -> 'sku' ->> 'sku_id' AS sku_sku_id,
  changes -> 'sku' ->> 'user_id' AS sku_user_id,
  changes -> 'sku' ->> 'approved' AS sku_approved,
  changes -> 'sku' ->> 'quantity' AS sku_quantity,
  changes -> 'sku' ->> 'inventory' AS sku_inventory,
  changes -> 'sku' ->> 'seller_id' AS sku_seller_id,
  changes -> 'sku' ->> 'product_id' AS sku_product_id,
  changes -> 'sku' ->> 'listed_date' AS sku_listed_date,
  changes -> 'sku' -> 'seller_change_requests' ->> 'expiry_date' AS sku_expiry_date,
  changes -> 'sku' -> 'seller_change_requests' ->> 'stock_availability_date' AS sku_stock_availability_date
FROM activity_logs al
where payload IN ('Product SKU Updated successfully',
'Item added successfully',
'Product deleted successfully',
'SKU Request updated successfully',
'Seller change request updated by admin',
'Out of stock marked manually',
'SKU updated successfully',
'Product updated successfully',
'Product SKU Updated successfully By Admin',
'Item updated successfully',
'Item deleted successfully',
'SKU updated successfully by retailer',
'Product SKU Detail created successfully',
'Product SKU created successfully')
) d
WHERE TO_TIMESTAMP(updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')::TIMESTAMP > DATE('2023-06-21 19:12:41.593000+00:00')