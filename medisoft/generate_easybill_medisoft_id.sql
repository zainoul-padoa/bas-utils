-- At this step, we got data from delta easybill_medisoft match

-- before any modif, backup
CREATE TABLE bas_firms.easybill_medisoft_backup_20260704HHMMSS AS 
  select * from bas_firms.easybill_medisoft;

-- if an Easybill has only 1 Medisoft child, then ID = Easybill_ID

-- first have a look : 
SELECT em.*, f.mandant FROM bas_firms.easybill_medisoft em 
LEFT JOIN table_firmenstruktur f ON f.rec_id = em.medisoft_id 
WHERE id <> easybill_id -- id different from easybill, where it should not, because we are in 1E x 1M case
AND easybill_id IN ( -- list of easybill_id having only 1 child
SELECT easybill_id
FROM bas_firms.easybill_medisoft
WHERE medisoft_id IS NOT NULL 
GROUP BY easybill_id
HAVING count(*) = 1 )
AND id IS NULL -- it is a newly injected firm, so it has no ID yet
AND lower(f.mandant) <> 'rostock' -- Rostock already in prod, we don't change them

-- now update
UPDATE bas_firms.easybill_medisoft 
SET id = easybill_id 
WHERE easybill_id IN (
  SELECT easybill_id FROM bas_firms.easybill_medisoft em 
  JOIN table_firmenstruktur f ON f.rec_id = em.medisoft_id 
  WHERE id <> easybill_id -- id different from easybill, where it should not, because we are in 1E x 1M case
  AND easybill_id IN ( -- list of easybill_id having only 1 child
  SELECT easybill_id
  FROM bas_firms.easybill_medisoft
  WHERE medisoft_id IS NOT NULL 
  GROUP BY easybill_id
  HAVING count(*) = 1 )
  AND lower(f.mandant) <> 'rostock'
  )
AND id IS NULL -- it is a newly injected firm, so it has no ID yet


-- else if 1E x nM we generate an ID

-- find max existing ID
-- ignore the 60xxxxx
SELECT * FROM bas_firms.easybill_medisoft
where id is not null
ORDER BY id DESC;

-- set sequence value to the max ID
-- ignore the 60xxxxx
SELECT setval('bas_firms.medisoft_independent', <max_id>);


UPDATE bas_firms.easybill_medisoft 
SET id = nextval('bas_firms.medisoft_independent')::text
WHERE id IS NULL
AND medisoft_id IS NOT NULL 
AND easybill_id IS NOT NULL


-- check they are correctly set
SELECT * FROM bas_firms.easybill_medisoft WHERE id::int > <ex_max_id>
ORDER BY id asc;


-- Inject Medisoft without Easybill match

-- first make sure cleaned_medisoft contains all firms without Easybill match
-- https://docs.google.com/spreadsheets/d/1zVmAmbUnvd-d0J98-cWvmdW_Ys5dpGk-klfNak_k9jI/edit?gid=825688280#gid=825688280
-- then inject them
INSERT INTO bas_firms.easybill_medisoft(id, medisoft_id)
SELECT  nextval('bas_firms.medisoft_independent')::text, medisoft_id FROM bas_firms.cleaned_medisoft cm WHERE no_migration IS FALSE AND has_easybill_connection IS FALSE
AND medisoft_id NOT IN ('00_A9D00RRGEY','00_A9I00GKTXI','00_A9C00VIX0B','00_A9500HE14X') -- already injected Rostock firms
