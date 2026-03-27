# SQL Query Library (Few-Shot Examples)

### Example 1: Comparing Internal Sales (Orders) with IMS Benchmarks
```sql
WITH Internal AS (
  SELECT ib."name", SUM(ms."product_quantity") as qty 
  FROM "master_sale" ms 
  JOIN "customer_details" cd ON ms."customer_id" = cd."customer_id" 
  JOIN "ims_brick" ib ON cd."ims_brick_id" = ib."id" 
  GROUP BY 1
),
Market AS (
  SELECT ib."name", SUM("unit") as qty 
  FROM "ims_sale" s 
  JOIN "ims_brick" ib ON s."brickId" = ib."id" 
  GROUP BY 1
)
SELECT 
  COALESCE(i."name", m."name") AS "Brick Name", 
  COALESCE(i.qty, 0) AS "Internal Units", 
  COALESCE(m.qty, 0) AS "IMS Market Units"
FROM Internal i 
FULL OUTER JOIN Market m ON i."name" = m."name"
ORDER BY "Internal Units" DESC
LIMIT 5;
```

### Example 2: Manager Visit Frequency
```sql
SELECT 
  m."name", 
  COUNT(dp."id") AS "Visit Count"
FROM "managers" AS m
JOIN "doctor_plan" AS dp ON m."id" = dp."managerId"
GROUP BY m."name"
ORDER BY "Visit Count" DESC;
```

### Example 3: Finding Doctors by Specialization and Region
```sql
SELECT 
  dr."name", 
  sp."name" AS "Specialization", 
  r."name" AS "Region"
FROM "doctors" AS dr
JOIN "specializatons" AS sp ON dr."id" = ANY(SELECT "B" FROM "_doctorsTospecializatons" WHERE "A" = dr."id") -- Handling many-to-many
JOIN "healthcentres" AS hc ON dr."id" = ANY(SELECT "A" FROM "_doctorsTohealthcentres" WHERE "B" = dr."id")
JOIN "regions" AS r ON hc."region_id" = r."id";
```
### Example 4: Counting Visits for a Specific Doctor (Handling subqueries and mixed case columns)
```sql
SELECT 
  d."name",
  COUNT(dp."id") AS "Doctor Visits"
FROM "doctors" d
JOIN "doctor_plan" dp ON dp."doctorId" = d."id"  -- Always quote camelCase columns like "doctorId"
WHERE d."id" IN (
    SELECT "id" FROM "doctors" WHERE "name" ILIKE '%faisal%' 
) -- Always use IN instead of '=' for subqueries to prevent 'more than one row returned' errors.
GROUP BY d."name"
ORDER BY "Doctor Visits" DESC;
### Example 5: Monthly Sales for a Specific Area Brick
```sql
SELECT 
  product_name, 
  SUM(product_quantity) AS total_qty, 
  SUM(total_amount) AS total_revenue
FROM master_sale
WHERE area_name ILIKE '%F.B.AREA BLOCK-14%'
  AND EXTRACT(MONTH FROM invoice_date) = 12
  AND EXTRACT(YEAR FROM invoice_date) = 2025
GROUP BY product_name
ORDER BY total_revenue DESC;
```
**CRITICAL NOTE:** Database `master_sale` uses `invoice_date`. The column `sale_date` does NOT exist.

### Example 9: Antibiotics Simulation (Product Group Analysis)
```sql
-- Monthly trend of Antibiotics in Karachi for Simulation
SELECT 
  product_group_name, 
  SUM(total_amount) as total_revenue,
  invoice_date
FROM master_sale 
WHERE product_group_name ILIKE '%Antibiotic%'
AND area_name ILIKE '%Karachi%'
GROUP BY 1, 3;
```
**CRITICAL:** Always use `product_group_name` (NOT `product_group`).
### Example 6: Filtering Sales by Manager Name
```sql
-- Direct way: Using master_sale columns (Fastest)
SELECT SUM(total_amount) 
FROM master_sale 
WHERE area_manager_name ILIKE '%Haider Ali%'
AND invoice_date >= '2025-01-01';
```
**CRITICAL NOTE:** `area_managers` does NOT have `distributor_id`. Always use columns in `master_sale` for geography filtering.
### Example 8: Top Doctor Sales (Pareto Analysis)
```sql
SELECT 
  customer_name AS "Doctor Name", 
  SUM(total_amount) AS "Total Sales"
FROM master_sale 
WHERE customer_type = 'Doctors'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```
**CRITICAL:** `master_sale` uses `customer_name` for doctors. NEVER use `doctor_name`.
