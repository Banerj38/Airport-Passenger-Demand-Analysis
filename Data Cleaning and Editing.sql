-------------Import Table--------------

create table T_T100_SEGMENT_ALL_CARRIER2019(
	DEPARTURES_SCHEDULED numeric,
	DEPARTURES_PERFORMED numeric,
	PAYLOAD numeric,
	SEATS numeric,
	PASSENGERS int,
	FREIGHT numeric,
	MAIL numeric,
	DISTANCE numeric,
	RAMP_TO_RAMP numeric,
	AIR_TIME numeric,
	UNIQUE_CARRIER text,
	AIRLINE_ID int,
	UNIQUE_CARRIER_NAME text,
	CARRIER text,
	CARRIER_NAME text,
	ORIGIN_AIRPORT_ID int,
	ORIGIN text,
	DEST_AIRPORT_ID int,
	DEST text,
	AIRCRAFT_GROUP int,
	AIRCRAFT_TYPE int,
	AIRCRAFT_CONFIG int,
	YEAR int,
	QUARTER int,
	MONTH int,
	CLASS text);

/*I used the import option in PGAdmin to import T_T100_SEGMENT_ALL_CARRIER2019 data*/

-------------Create footfall_lat_long-----------------

Create table footfall as 
	(with o as
		(select origin, sum(passengers) as OutPax
		from T_T100_SEGMENT_ALL_CARRIER2019
		where origin <> dest and class = 'F'
		group by origin),
		
	 d as
		(select dest, sum(passengers) as InPax
		from T_T100_SEGMENT_ALL_CARRIER2019
		where origin <> dest and class = 'F'
		group by dest)	
	 
	 select o.origin, d.dest, d.InPax, o.OutPax
	 from o full outer join d
	 on o.origin = d.dest);

alter table footfall
add airport_code text;

UPDATE footfall
SET airport_code =
        CASE WHEN origin is null
            THEN dest
            ELSE origin
        END;

update footfall set InPax = 0 where InPax is null;
update footfall set OutPax = 0 where OutPax is null;	 

DELETE FROM footfall where InPax = 0 and OutPax = 0;

--Import airport_code data--

Create table airport_name
	(code text,
	airport_name text);

COPY airport_name
FROM 'F:\Airport_Data\Data\T100_BTS\L_AIRPORT.csv' 
DELIMITER ',' 
CSV HEADER;

--Import lat_long data--

Create table lat_long
	(code text,
	latitude numeric,
	longitude numeric);

COPY lat_long
FROM 'F:\Airport_Data\Data\T_MASTER_CORD.csv' 
DELIMITER ',' 
CSV HEADER;

--Remove repetative lat_long data and join all the data--

create table footfall_lat_long as
	(select distinct on (airport_code)
	airport_code, airport_name, latitude, longitude, InPax, OutPax
	from
		(select f.airport_code, a.airport_name, l.latitude, l.longitude, f.InPax, f.OutPax
		from footfall f
		inner join airport_name a
		on f.airport_code = a.code
		inner join lat_long l
		on f.airport_code = l.code) as b
	order by airport_code);

COPY footfall_lat_long TO 'F:\Airport_Data\Output\footfall_lat_long.csv'  WITH DELIMITER ',' CSV HEADER;



-----------Create route_data---------------

/*
OD12 and OD21 are tables where you'll find aggregate values of number of seats, passengers in the route
and distance in miles of the route. The numbers are not rounded.

OD12 - I aggregated the table for flights originating from A to B
OD21 - This table essentially shows the aggregated data of the return flights from B to A

UID - I created unique id for each flight routes, eg - for flighths A to B or B to A, the route ID is AB
ID - it is column ID 
*/

create table od12 as
	(select origin, dest, sum(distance)/count(distance) as distance, concat (origin,'_',dest) as OD12 , 
 	sum(seats) as Seats12, sum (passengers) as NPax12
	from T_T100_SEGMENT_ALL_CARRIER2019
	where class = 'F'
	group by origin, dest
	order by origin, dest); /* 19227 */

ALTER TABLE od12
ADD uid text;

UPDATE od12
SET uid =
        CASE WHEN origin < dest
            THEN concat (origin, dest)
            ELSE concat (dest, origin)
        END;

ALTER TABLE OD12 ADD COLUMN id bigserial;
		
		
create table od21 as
(select dest, origin, concat (origin,'_' ,dest) as OD21 , sum(seats) as Seats21, sum (passengers) as NPax21 
 from T_T100_SEGMENT_ALL_CARRIER2019
 where class = 'F'
group by dest,origin
order by dest,origin); /*19227*/

ALTER TABLE od21
ADD uid text;

UPDATE od21
SET uid =
        CASE WHEN origin < dest
            THEN concat (origin, dest)
            ELSE concat (dest, origin)
        END;


   
--Joined OD12 and OD21 based on UID--

create table OD_Data1 as 
(select od12.origin, od12.dest, distance, od12, seats12, npax12, od12.uid, od21, seats21, npax21, od12.id 
from od12
inner join od21
on od12.uid = od21.uid
order by od12.id); /* 33237 */


/* In the following few steps, some data where destination and origin are the same, 
where the od12 and od21 pair are the same have been filteres
OD_Data2 -  data where origin and destination are not repeated in OD12 and OD21 data
*/

create table OD_Data2 as 
(select origin, dest, distance, od12, seats12, npax12, uid, od21, seats21, npax21, 
 round((npax21+npax12)/365/2,0) as NPax1way_perday, id
from od_data1
where od12<>od21); /*14010*/

DELETE FROM OD_Data2
WHERE id NOT IN (
   select min(id) from OD_Data2
   group by uid);


--OD_Data3 is filtered data only for routes with no return flights--

create table OD_Data3 as 
(select origin, dest, distance, od12, od21, seats12, npax12, uid, npax12/365 as NPax1way_perday
from od_data1
where od12=od21 and origin<>dest);/*19105*/


DELETE FROM OD_Data3
WHERE uid NOT IN (
  select uid from od_data3
	group by uid
having count(uid)=1);


alter table OD_Data3
drop uid,
drop od21;

ALTER TABLE OD_Data3
ADD od21 text,
add seats21 numeric,
add npax21 numeric;

/*
OD_Data4 id table for flights having same origin and destination. As origin=destination makes no sense, this table was
made just for reference. The data from this table is not included in any final task tables.
*/

create table OD_Data4 as 
(select origin, dest, distance, od12, seats12, npax12, uid, od21, npax12/365 as NPax1way_perday
from od_data1
where od12=od21 and origin=dest);


alter table OD_Data4
drop uid,
drop od21;

ALTER TABLE OD_Data4
ADD od21 text,
add seats21 numeric,
add npax21 numeric;

/*
route_data is where the information is aggregated for all the routes in 
T_T100_SEGMENT_ALL_CARRIER2019 data, except OD_Data4 routes.
*/

create table route_data as
	(select  origin, dest, round(distance, 0) as distance, od12, round(seats12,0) as seats12, 
	 round(npax12,0) as npax12, od21, round(seats21, 0) as seat21, round(npax21,0) as npax21, 
	 round(NPax1way_perday,0) as NPax1way_perday
	from od_data2
	union all
	select origin, dest, round(distance, 0) as distance, od12, round(seats12,0) as seats12, 
	 round(npax12,0) as npax12, od21, round(seats21, 0) as seat21, round(npax21,0) as npax21, 
	 round(NPax1way_perday,0) as NPax1way_perday
	 from od_data3);

COPY route_data TO 'F:\Airport_Data\Output\route_data.csv'  WITH DELIMITER ',' CSV HEADER;

	
--------------------- End ----------------------
