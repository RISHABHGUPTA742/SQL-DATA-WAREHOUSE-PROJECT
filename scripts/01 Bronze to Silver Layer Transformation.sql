-- Databricks notebook source
SELECT * FROM `datawerhouse1`.`bronze`.`customers_txt`;

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.delivery_events LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.driver_monthly_metrics LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.facilities LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.drivers LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.fuel_purchases LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.loads LIMIT 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.maintenance_records LIMIT 1000

-- COMMAND ----------

  select * from datawerhouse1.bronze.routes limit 1000

-- COMMAND ----------

SELECT * FROM datawerhouse1.bronze.safety_incidents

-- COMMAND ----------

select * from datawerhouse1.bronze.trailers

-- COMMAND ----------

select * from datawerhouse1.bronze.truck_utilization_metrics

-- COMMAND ----------

select * from datawerhouse1.bronze.trips

-- COMMAND ----------

select * from datawerhouse1.bronze.trucks  

-- COMMAND ----------

---cheack null or duplicate in primary key
select
 customer_id as customer,
count(*)
 from datawerhouse1.bronze.customers_txt
 group by customer_id
 having count(*) > 1

-- COMMAND ----------

---cheack null and duplicate and primary key
select 
    event_id as event, 
    load_id as load, 
    trip_id as trip, 
    facility_id as facility,
    count(*)
from datawerhouse1.bronze.delivery_events
group by event_id, load_id, trip_id, facility_id
having count(*) > 2

union all

select 
    'NULL' as event,
    'NULL' as load,
    'NULL' as trip,
    'NULL' as facility,
    count(*)
from datawerhouse1.bronze.delivery_events
where event_id is null or load_id is null or trip_id is null or facility_id is null

-- COMMAND ----------

-- check the null and duplicate in primary key
select 
maintenance_id as maintenance,
truck_id as truck,
count(*)
from datawerhouse1.bronze.maintenance_records
where maintenance_id is null or truck_id is null
group by maintenance_id, truck_id
having count(*) > 2

-- COMMAND ----------

---check the null and duplicate primary key
select 
route_id as route, 
count(*) 
from datawerhouse1.bronze.routes
group by route_id
having count(*) > 1
union all
select 
'NULL' as route,
count(*) 
from datawerhouse1.bronze.routes
where route_id is null

-- COMMAND ----------

--check null and duplicate in primary key
---expectation = no result
select * from datawerhouse1.bronze.safety_incidents;
select 
       incident_id as incident,
       trip_id as trip,
       truck_id as truck,
       driver_id as driver,
       count(*) 
 from datawerhouse1.bronze.safety_incidents
 group by incident_id, trip_id, truck_id, driver_id
 having count(*) > 1
union all
select
    'NULL' as incident,
    'NULL' as trip,
    'NULL' as truck,
    'NULL' as driver,
    count(*)
from datawerhouse1.bronze.safety_incidents
where incident_id is null;

select 
incident_id , trip_id , truck_id , driver_id 
from datawerhouse1.bronze.safety_incidents
where incident_id is not null

-- COMMAND ----------

-----check the null and duplicate in primary key
---result = 0
select 
trailer_id as trailer,
count(*)
 from datawerhouse1.bronze.trailers
 group by trailer_id
 having count(*) > 1;
 select  'null' as trailer_id from datawerhouse1.bronze.trailers
 where trailer_id is null;
 select * from datawerhouse1.bronze.trailers
 where trailer_id is null;


-- COMMAND ----------

----check the duplicate and null values
---result = 0
select 
trip_id as trip,
load_id as load,
driver_id as driver,
truck_id as truck,
trailer_id as trailer,
count(*)
from datawerhouse1.bronze.trips
group by trip_id, load_id, driver_id, truck_id, trailer_id
having count(*) > 1;

select * from datawerhouse1.bronze.trips
where trip_id is null 
   or load_id is null 
   or driver_id is null 
   or truck_id is null 
   or trailer_id is null;
   select * from datawerhouse1.bronze.trips
   where trip_id = 'TRIP00000067'

-- COMMAND ----------

select 
    driver_id as driver,
    count(*)
 from datawerhouse1.bronze.drivers
 group by driver_id
 having count(*) > 1;
 select * from datawerhouse1.bronze.drivers
 where driver_id is null;
 select * from datawerhouse1.bronze.drivers
 where driver_id = 'DRV00011';
 select * from datawerhouse1.bronze.drivers

-- COMMAND ----------

select * from datawerhouse1.bronze.driver_monthly_metrics
where driver_id is null;

select 
    driver_id,
    count(*) 
from datawerhouse1.bronze.driver_monthly_metrics
group by driver_id
having count(*) > 1

union all

select 
    'NULL' as driver_id,
    count(*)
from datawerhouse1.bronze.driver_monthly_metrics
where driver_id is null;

-- COMMAND ----------

select 
      facility_id,
      count(*)
from datawerhouse1.bronze.facilities
group by facility_id
having count(*) > 1

union all

select 
      'NULL' as facility_id,
      count(*)
from datawerhouse1.bronze.facilities
where facility_id is null

-- COMMAND ----------

select 
     fuel_purchase_id,
     trip_id,
     truck_id,
     driver_id,
     count(*) 
from datawerhouse1.bronze.fuel_purchases
group by 
      fuel_purchase_id,
      trip_id,
      truck_id,
      driver_id
having count(*) > 1

union all

select 
     'NULL' as fuel_purchase_id,
     'NULL' as trip_id,
     'NULL' as truck_id,
     'NULL' as driver_id,
     count(*)
from datawerhouse1.bronze.fuel_purchases
where fuel_purchase_id is null

-- COMMAND ----------

select * from datawerhouse1.bronze.fuel_purchases
where facility_id is null

-- COMMAND ----------

select 
     load_id,
     customer_id,
     route_id, 
     count(*)
from datawerhouse1.bronze.loads
group by 
     load_id,
     customer_id,
     route_id 
having count(*) > 1

union all

select 
     'NULL' as load_id,
     'NULL' as customer_id,
     'NULL' as route_id,
     count(*)
from datawerhouse1.bronze.loads
where load_id is null or customer_id is null or route_id is null

-- COMMAND ----------

select 
    truck_id,
    month,
    count(*)
from datawerhouse1.bronze.truck_utilization_metrics
group by truck_id, month
having count(*) > 1

union all

select 
    'NULL' as truck_id,
    try_cast(NULL as date) as month,
    count(*)
from datawerhouse1.bronze.truck_utilization_metrics
where month is null;

select * from datawerhouse1.bronze.truck_utilization_metrics
where try_cast(month as date) is null;
