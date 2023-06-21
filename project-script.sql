-- Script for visualizations by Julia (figures 5, 6, and 7)

select * from (
	select zipcode, count(*) as total_homes
	from Buildings
	where PID in (select PID from Assessments where year=2023)
	group by zipcode
)
where total_homes>=50

-- group by ward into different views 
-- for each ward, group by year and find average property value

drop view if exists new_PID; -- adds leading zero to PID

create view new_PID as
select FORMAT('%010d', PID) as PID, year, LV, BV
from Assessments;

-- Revised query for figure 3
select SUBSTRING(PID, 1, 2) AS ward, ROUND(AVG(LV + BV)) AS average_price, year, PID
from (
	select *
	from new_PID
	where year=2023
	)
where LV > 0
	and BV > 0
group by ward
order by average_price desc;

-- view that separates by ward in new column
drop view if exists ward;

create view ward as
select *, SUBSTRING(PID, 1, 2) AS ward
from new_PID
where LV > 0
	and BV > 0;

select * from ward;

-- ward 1 change 
drop view if exists ward1;

create view ward1 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="01"
group by year

-- ward 2 change 
drop view if exists ward2;

create view ward2 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="02"
group by year

-- ward 3 change 
drop view if exists ward3;

create view ward3 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="03"
group by year

-- ward 4 change 
drop view if exists ward4;

create view ward4 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="04"
group by year

-- ward 5 change 
drop view if exists ward5;

create view ward5 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="05"
group by year

-- ward 6 change 
drop view if exists ward6;

create view ward6 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="06"
group by year

-- ward 7 change 
drop view if exists ward7;

create view ward7 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="07"
group by year

-- ward 8 change 
drop view if exists ward8;

create view ward8 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="08"
group by year

-- ward 9 change 
drop view if exists ward9;

create view ward9 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="09"
group by year

-- ward 10 change 
drop view if exists ward10;

create view ward10 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="10"
group by year

-- ward 11 change 
drop view if exists ward11;

create view ward11 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="11"
group by year

-- ward 12 change 
drop view if exists ward12;

create view ward12 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="12"
group by year

-- ward 13 change 
drop view if exists ward13;

create view ward13 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="13"
group by year

-- ward 14 change 
drop view if exists ward14;

create view ward14 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="14"
group by year

-- ward 15 change 
drop view if exists ward15;

create view ward15 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="15"
group by year

-- ward 16 change
drop view if exists ward16;

create view ward16 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="16"
group by year

-- ward 17 change 
drop view if exists ward17;

create view ward17 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="17"
group by year

-- ward 18 change 
drop view if exists ward18;

create view ward18 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="18"
group by year

-- ward 19 change
drop view if exists ward19;

create view ward19 as 
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="19"
group by year

-- ward 20 change 
drop view if exists ward20;

create view ward20 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="20"
group by year

-- ward 21 change 
drop view if exists ward21;

create view ward21 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="21"
group by year

-- ward 22 change 
drop view if exists ward22;

create view ward22 as
select ROUND(AVG(LV + BV)) AS average_price, year, PID
from ward
where ward="22"
group by year

select * from Buildings



