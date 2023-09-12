SELECT 
    verticals.name AS "Vertical", 
    categories.name AS "Category Name", 
    sub_categories.name AS "Sub-Category Name",
    subsub_categories.name AS "SubSub-Category Name",
    product_sku.id AS "SKU ID",
    product_sku.__name AS "SKU Name",
    product_variants.name AS "Product Variant Name",
    product_sku.price as "Price",    
    brands.name as "brand_name",
    m.name as "manufacturer_name",
    users.id AS "Supplier ID",
    users.name AS "Supplier Name",
    suppliers.store_name AS "Supplier Store Name"
FROM 
    product_sku
LEFT JOIN 
    product_variants ON product_sku.product_variant_id = product_variants.id 
LEFT JOIN 
    products ON product_sku.product_id = products.id 
LEFT JOIN 
    product_sku_details ON product_sku.id = product_sku_details.sku_id 
LEFT JOIN 
    users ON product_sku_details.user_id = users.id 
LEFT JOIN 
    suppliers ON users.id = suppliers.user_id 
LEFT JOIN 
    cities ON suppliers.city_id = cities.id 
LEFT JOIN 
    verticals ON products.vertical_id = verticals.id 
LEFT JOIN 
    product_categories ON products.id = product_categories.product_id 
LEFT JOIN 
    categories ON product_categories.category_id = categories.id 
LEFT JOIN 
    categories AS sub_categories ON categories.id = sub_categories.parent_id
LEFT JOIN 
    categories AS subsub_categories ON sub_categories.id = subsub_categories.parent_id
left join 
 	product_brands on product_brands.product_id= products.id
LEFT JOIN 
    brands on brands.id=product_brands.brand_id 
LEFT JOIN 
    product_manufacturing pm on products.id = pm.product_id 
LEFT JOIN 
    manufacturing m on m.id = pm.manufacturing_id