-- table uberregional_id
CREATE TABLE ue_id AS 
SELECT distinct mother_id, padoa_id  FROM bas_firms.uberregional_entity ue ; 

-- rajout de la colonne id
ALTER TABLE ue_id ADD COLUMN id text;

-- on met la séquence à id max + 500
SELECT setval('bas_firms.easybill_merge_seq', 130030401); 

-- on incrémente pour chaque ligne de ue_id
UPDATE ue_id 
SET id = nextval('bas_firms.easybill_merge_seq')

-- on rajoute la colonne padoa_final_id 
ALTER TABLE bas_firms.uberregional_entity  
ADD COLUMN padoa_final_id text;

ALTER TABLE ue_id 
RENAME COLUMN padoa_id TO old_padoa_id;

-- before any modification, backup
CREATE TABLE bas_firms.uberregional_entity_backup_20260706_1328 AS 
SELECT * FROM bas_firms.uberregional_entity;


-- give generated ID to the couple mother_id - padoa_id
UPDATE bas_firms.uberregional_entity ue
SET padoa_final_id = (SELECT uei.id FROM ue_id uei WHERE uei.mother_id = ue.mother_id AND uei.old_padoa_id = ue.padoa_id)
; 

-- oops we generated an ID when mother_id = padoa_id too, we shouldn't have
-- fix it
-- padoa_final_id = mother_id in that case
UPDATE bas_firms.uberregional_entity 
SET padoa_final_id = mother_id 
WHERE padoa_id = mother_id;
