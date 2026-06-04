-- on reprend les anciens ID
UPDATE bas_firms.easybill_medisoft em
SET id = (SELECT s.id FROM bas_firms_2026_04.easybill_medisoft_save_2 s WHERE COALESCE(s.easybill_id,'') = coalesce(em.easybill_id, '') AND coalesce(s.medisoft_id , '') = coalesce(em.medisoft_id , '')  ) ; 

-- quels Easybill_ID ont 1 seul Medisoft match
SELECT easybill_id, count(*) 
FROM bas_firms.easybill_medisoft em 
GROUP BY easybill_id 
HAVING count(*) = 1

-- dans ces cas, ID = Easybill_ID 
UPDATE bas_firms.easybill_medisoft em
SET id = easybill_id 
WHERE easybill_id IN (SELECT easybill_id
FROM bas_firms.easybill_medisoft em 
GROUP BY easybill_id 
HAVING count(*) = 1)

-- quels Easybill_ID ont N Medisoft matches
SELECT easybill_id, count(*) 
FROM bas_firms.easybill_medisoft em 
GROUP BY easybill_id 
HAVING count(*) > 1

-- max value of ID
SELECT * FROM bas_firms.easybill_medisoft em WHERE id IS NOT NULL ORDER BY id DESC;
-- set to that value
SELECT setval('bas_firms.easybill_medisoft_id_seq',130007945)

-- on génère un ID quand le ID est null et que le Easybill_id a plusieurs Medisoft matches
UPDATE bas_firms.easybill_medisoft em 
SET id = nextval('bas_firms.easybill_medisoft_id_seq')
WHERE id IS NULL 
AND easybill_id IN (SELECT easybill_id
FROM bas_firms.easybill_medisoft em 
GROUP BY easybill_id 
HAVING count(*) > 1 )