# SQL Query Library (Few-Shot Examples)

### Example 1: Comparing Internal Sales (Orders) with IMS Benchmarks
```sql
SELECT 
  b."name" AS "Brick Name", 
  SUM(CAST(od."product_quantity" AS numeric)) AS "Internal Units", 
  SUM(ims."unit") AS "IMS Market Units"
FROM "ims_brick" AS b
JOIN "ims_sale" AS ims ON b."id" = ims."brickId"
LEFT JOIN "customer_details" AS cd ON b."id" = cd."ims_brick_id"
LEFT JOIN "orders" AS o ON cd."customer_id" = o."customer_id"
LEFT JOIN "order_details" AS od ON o."id" = od."order_id"
GROUP BY b."name"
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
