-- faut que je fasse de l'ordre

SELECT count(*) FROM public.easybill_medisoft_backup_20260624_1648 WHERE id IS NULL;

SELECT * FROM bas_firms.easybill_medisoft em WHERE id = '130026172';



SELECT * FROM bas_firms.easybill_medisoft WHERE id::int > 130026815 AND id::int < 130026983
ORDER BY id asc;


SELECT * FROM bas_firms.easybill_medisoft WHERE medisoft_id IN (
SELECT cm.medisoft_id FROM bas_firms.cleaned_medisoft cm WHERE no_migration IS FALSE AND has_easybill_connection IS FALSE 
aND medisoft_id NOT IN ('00_A9D00RRGEY','00_A9I00GKTXI','00_A9C00VIX0B','00_A9500HE14X')
);


INSERT INTO bas_firms.easybill_medisoft(id, medisoft_id)
SELECT  nextval('bas_firms.medisoft_independent')::text, medisoft_id FROM bas_firms.cleaned_medisoft cm WHERE no_migration IS FALSE AND has_easybill_connection IS FALSE
AND medisoft_id NOT IN ('00_A9D00RRGEY','00_A9I00GKTXI','00_A9C00VIX0B','00_A9500HE14X');


SELECT * FROM bas_firms.easybill_medisoft;

select * from pg_stat_file( pg_relation_filepath( 'bas_firms.cleaned_medisoft' ) ) ; 

SELECT setval('bas_firms.medisoft_independent', 130026815);


SELECT * FROM 
bas_firms.easybill_medisoft
ORDER BY id DESC;

UPDATE bas_firms.easybill_medisoft 
SET id = nextval('bas_firms.medisoft_independent')::text
WHERE id IS NULL;


CREATE TABLE public.easybill_medisoft_backup_20260624_2026 AS 
SELECT * FROM bas_firms.easybill_medisoft
;

CREATE TABLE bas_firms.easybill_medisoft_backup_20260624_1648 AS 
SELECT * FROM public.easybill_medisoft_backup_20260624_1648;

ALTER TABLE bas_firms.easybill_medisoft RENAME TO easybill_medisoft_bad;

ALTER TABLE bas_firms.easybill_medisoft_backup_20260624_1648 RENAME TO easybill_medisoft ;
