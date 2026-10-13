--------------------------- firm_source_id -----------------------
-- Create Easybill_Medisoft_2
CREATE table bas_firms.easybill_medisoft_2 as select * from bas_firms.easybill_medisoft ; 

-- check min and max id
select min(id), max(id) from bas_firms.easybill_medisoft_2
;

-- first change type of id to bigint
ALTER TABLE bas_firms.easybill_medisoft_2 
ALTER COLUMN id TYPE bigint;

-- IDs range from 15781 to 19731. Change it to range from 130004001 to 130007951
UPDATE bas_firms.easybill_medisoft_2
SET id = id - 15780 + 130004000
;

UPDATE bas_firms.easybill_medisoft_2
SET id = easybill_id 
WHERE easybill_id IN (
WITH cte AS (SELECT easybill_id, count(*) FROM bas_firms.easybill_medisoft em 
GROUP BY easybill_id 
HAVING count(*) = 1)
SELECT easybill_id FROM cte
 ) ; 

-- Check 
select min(id), max(id) from bas_firms.easybill_medisoft_2
;
SELECT * FROM bas_firms.easybill_medisoft_2 em WHERE id = '130005500';
