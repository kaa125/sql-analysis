SELECT *
FROM (
  SELECT
  (changes -> 'current' ->> 'id')::float AS id,
  (changes -> 'current' ->> 'mrp')::float AS mrp,
  (changes -> 'current' ->> 'code') AS code,
  (changes -> 'current' ->> 'rank')::float AS rank,
  (changes -> 'current' ->> 'price')::float AS price,
  (changes -> 'current' ->> 'is_hot')::boolean AS is_hot,
  (changes -> 'current' ->> 'weight')::float AS weight,
  (changes -> 'current' ->> 'barcode') AS barcode,
  (changes -> 'current' ->> 'is_aoos')::boolean AS is_aoos,
  (changes -> 'current' ->> 'is_bulk')::boolean AS is_bulk,
  (changes -> 'current' ->> 'is_deal')::boolean AS is_deal,
  (changes -> 'current' ->> 'discount')::float AS discount,
  (changes -> 'current' ->> 'is_stock')::boolean AS is_stock,
  (changes -> 'current' ->> 'cities_id')::float AS cities_id,
  (changes -> 'current' ->> 'is_active')::boolean AS is_active,
  (changes -> 'current' ->> 'max_limit')::float AS max_limit,
  (changes -> 'current' ->> 'min_limit')::float AS min_limit,
  (changes -> 'current' ->> 'aoos_limit')::float AS aoos_limit,
  (changes -> 'current' ->> 'best_price')::boolean AS best_price,
  (changes -> 'current' ->> 'created_at')::timestamp AS created_at,
  (changes -> 'current' ->> 'deleted_at')::timestamp AS deleted_at,
  (changes -> 'current' ->> 'deleted_by')::float AS deleted_by,
  (changes -> 'current' ->> 'is_deleted')::boolean AS is_deleted,
  (changes -> 'current' ->> 'is_visible')::boolean AS is_visible,
  (changes -> 'current' ->> 'product_id')::float AS product_id,
  (changes -> 'current' ->> 'updated_at')::timestamp AS updated_at,
  (changes -> 'current' ->> 'updated_by')::float AS updated_by,
  (changes -> 'current' ->> 'is_new_label')::boolean AS is_new_label,
  (changes -> 'current' ->> 'back_in_stock')::boolean AS back_in_stock,
  (changes -> 'current' ->> 'delivery_date')::date AS delivery_date,
  (changes -> 'current' ->> 'current_aoos_inv')::float AS current_aoos_inv,
  (changes -> 'current' ->> 'fulfilment_mode_id')::float AS fulfilment_mode_id,
  (changes -> 'current' ->> 'product_variant_id')::float AS product_variant_id,
  (changes -> 'sku' ->> 'id')::float AS sku_id,
  (changes -> 'sku' ->> 'price')::float AS sku_price,
  (changes -> 'sku' ->> 'sku_id') AS sku_sku_id,
  (changes -> 'sku' ->> 'user_id')::float AS sku_user_id,
  (changes -> 'sku' ->> 'approved')::boolean AS sku_approved,
  (changes -> 'sku' ->> 'quantity')::float AS sku_quantity,
  (changes -> 'sku' ->> 'inventory')::float AS sku_inventory,
  (changes -> 'sku' ->> 'seller_id')::float AS sku_seller_id,
  (changes -> 'sku' ->> 'product_id')::float AS sku_product_id,
  (changes -> 'sku' ->> 'listed_date')::date AS sku_listed_date,
  (changes -> 'sku' -> 'seller_change_requests' ->> 'expiry_date')::date AS sku_expiry_date,
  (changes -> 'sku' -> 'seller_change_requests' ->> 'stock_availability_date')::date AS sku_stock_availability_date
  FROM activity_logs al
  WHERE payload IN (
    'Product SKU Updated successfully',
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
    'Product SKU created successfully'
  )
) AS d
WHERE EXTRACT(MONTH FROM updated_at)=6

select * FROM(
SELECT (changes -> 'current' ->> 'id'),(changes -> 'current' ->> 'id')::float AS id_a
FROM activity_logs al 
WHERE changes -> 'current' ->> 'id' IS NOT NULL
) as d
where id_a ~ '^[-+]?[0-9]*\.?[0-9]+$'
