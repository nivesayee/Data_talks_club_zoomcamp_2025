select count(*) from greentripdata 
where lpep_pickup_datetime>='2019-10-01' 
and lpep_dropoff_datetime<'2019-11-01' 
and trip_distance<=1;

select count(*) from greentripdata 
where lpep_pickup_datetime>='2019-10-01' 
and lpep_dropoff_datetime<'2019-11-01' 
and trip_distance>1 and trip_distance<=3;

select count(*) from greentripdata 
where lpep_pickup_datetime>='2019-10-01' 
and lpep_dropoff_datetime<'2019-11-01' 
and trip_distance>3 and trip_distance<=7;

select count(*) from greentripdata 
where lpep_pickup_datetime>='2019-10-01' 
and lpep_dropoff_datetime<'2019-11-01' 
and trip_distance>7 and trip_distance<=10;

select count(*) from greentripdata 
where lpep_pickup_datetime>='2019-10-01' 
and lpep_dropoff_datetime<'2019-11-01' 
and trip_distance>10;

select date(lpep_pickup_datetime) from greentripdata 
where trip_distance=(select max(trip_distance) from greentripdata);

select "Zone" from greentripdata g join lookup l
on g."PULocationID"=l."LocationID"
where date(lpep_pickup_datetime)='2019-10-18'
group by "Zone"
having sum(total_amount)>13000;

with cte as (select g."DOLocationID" from greentripdata g join lookup l
on g."PULocationID"=l."LocationID"
where to_char(lpep_pickup_datetime,'yyyy-mm') = '2019-10'
and "Zone" = 'East Harlem North'
order by tip_amount desc limit 1)
select "Zone" from cte join lookup l on cte."DOLocationID"=l."LocationID";