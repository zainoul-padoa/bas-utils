-- We want to merge easybill_id based on what BAS told us
-- Here we update tables to replace old src easybill_id with dest easybill_id (aka merge_mother)

-- tables to update : 
-- bas_firms.easybill_medisoft
-- zoho.Deals
-- bas_firms.easybill_zoho
-- bas_firms.full_basic_care
-- bas_firms.basic_care_details
-- easybill.contacts
-- easybill.documents
-- bas_firms.employer_users

CREATE TABLE bas_firms.easybill_medisoft_save_2 AS
SELECT * FROM bas_firms.easybill_medisoft ;

CREATE TABLE bas_firms.easybill_zoho_save AS 
SELECT * FROM bas_firms.easybill_zoho; 

CREATE TABLE bas_firms.full_basic_care_save AS 
SELECT * FROM bas_firms.full_basic_care ;

CREATE TABLE bas_firms.basic_care_details_save AS 
SELECT * FROM bas_firms.basic_care_details ;

CREATE TABLE easybill.contacts_save AS 
SELECT * FROM easybill.contacts ; 

CREATE TABLE easybill.documents_save AS 
SELECT * FROM easybill.documents ;

CREATE TABLE bas_firms.employer_users_save AS 
SELECT * FROM bas_firms.employer_users ;

CREATE TABLE zoho."Deals_save" AS 
SELECT * FROM zoho."Deals";

---------------------------------------------------------------------------------------------------------
-- EASYBILL_MEDISOFT
-- if src_id is associated to NO Medisoft, we can delete the row, because dest_id will replace it
-- 10 rows
DELETE FROM bas_firms.easybill_medisoft em WHERE em.easybill_id in
(SELECT easybill_id  FROM bas_firms.easybill_clean ec WHERE ec."merge" AND ec.easybill_id <> ec.merge_mother  ) 
AND em.medisoft_id IS NULL; -- NO Medisoft
---------------------------------
-- if src_id is associated to 1 Medisoft, but dest_id is associated to no Medisoft
-- delete the dest_id from easybill_medisoft table since it does not carry any information, (3)
-- and update em.easybill_id with src_id -> dest_id (2)
-- also update id : src_id -> dest_id when id = easybill_id only (1)
-- implementation is in reverse order of the steps, in order not to lose the information "where dest.medisoft_id is null"
-- (1)
-- 4 rows
UPDATE bas_firms.easybill_medisoft em 
SET id = (SELECT DISTINCT -- in case 2 lines have the same mother
merge_mother FROM bas_firms.easybill_clean ec WHERE ec.easybill_id = em.easybill_id )
WHERE id IN (
    SELECT src.easybill_id FROM bas_firms.easybill_clean ec 
    JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
    JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
    WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
    AND dest.medisoft_id IS NULL
    ) ; 
-- em.id 130000117 will be replaced by em.id 113010075

-- (2)
-- 6 rows
UPDATE bas_firms.easybill_medisoft em 
SET easybill_id = (SELECT DISTINCT -- in case 2 lines have the same mother
merge_mother FROM bas_firms.easybill_clean ec WHERE ec.easybill_id = em.easybill_id )
WHERE easybill_id IN (
    SELECT src.easybill_id FROM bas_firms.easybill_clean ec 
    JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
    JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
    WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
    AND dest.medisoft_id IS NULL
    ) ; 
-- 130000090 will be replaced by 108040031

-- 5 rows
-- (3)
DELETE FROM bas_firms.easybill_medisoft 
WHERE id IN (
    SELECT dest.id FROM bas_firms.easybill_clean ec 
    JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother
    WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
    AND dest.medisoft_id IS NULL
    ) 
AND medisoft_id IS null
---------------------------------
-- if src_id and dest_id are both associated to Medisoft
-- we have 4 cases
-- a) 1 dest_id - 1 src_id -> replace dest and src em.id with null, we'll increment it later, and update src easybill_id -> dest_id
-- b) 1 dest_id - N src_id : replace dest em.id with null, to be incremented, and update src easybill_id -> dest_id
-- c) N dest_id - 1 src_id : replace src em.id with null, to be incremented, and update src easybill_id -> dest_id
-- d) N dest_id - N src_id : this does not happen, we can check with this query: (if it happened, keep em.id for both, and update src easybill_id -> dest_id)
SELECT count(DISTINCT dest.id) dest_cnt, dest.easybill_id , count(DISTINCT src.id) src_cnt FROM bas_firms.easybill_clean ec 
JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
AND dest.medisoft_id IS NOT NULL
AND src.medisoft_id IS NOT NULL  
GROUP BY dest.easybill_id ;
-- in all cases, we update src easybill_id -> dest_id, so this will be done at the end for everybody (1)
-- we will also increment for all null at the end (2)
-- a) 1 dest_id - 1 src_id -> replace dest and src em.id with null, we'll increment it later, and update src easybill_id -> dest_id
-- 2 rows
UPDATE bas_firms.easybill_medisoft em2
SET id = NULL  -- replace dest and src em.id with null
WHERE id IN (
    WITH cte AS (
        SELECT count(DISTINCT dest.id) dest_cnt, dest.easybill_id , count(DISTINCT src.id) src_cnt FROM bas_firms.easybill_clean ec 
        JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
        JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
        WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
        AND dest.medisoft_id IS NOT NULL
        AND src.medisoft_id IS NOT NULL  
        GROUP BY dest.easybill_id)
    SELECT em.id -- dest.id
    FROM cte JOIN bas_firms.easybill_medisoft em ON em.easybill_id = cte.easybill_id 
    WHERE dest_cnt = 1 AND src_cnt = 1 -- 1 : 1
    UNION 
    SELECT em.id -- src.id
    FROM cte JOIN bas_firms.easybill_clean ec ON ec.merge_mother = cte.easybill_id 
    JOIN bas_firms.easybill_medisoft em ON em.easybill_id = ec.easybill_id 
    WHERE ec.merge_mother <> ec.easybill_id 
    and dest_cnt = 1 AND src_cnt = 1 -- 1 : 1
)

-- b) 1 dest_id - N src_id : replace dest em.id with null, to be incremented, and update src easybill_id -> dest_id
-- 1 row
UPDATE bas_firms.easybill_medisoft em2
SET id = NULL 
WHERE id IN (
    WITH cte AS (
        SELECT count(DISTINCT dest.id) dest_cnt, dest.easybill_id , count(DISTINCT src.id) src_cnt FROM bas_firms.easybill_clean ec 
        JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
        JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
        WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
        AND dest.medisoft_id IS NOT NULL
        AND src.medisoft_id IS NOT NULL  
        GROUP BY dest.easybill_id)
    SELECT em.id -- dest.id
    FROM cte JOIN bas_firms.easybill_medisoft em ON em.easybill_id = cte.easybill_id 
    WHERE dest_cnt = 1 AND src_cnt > 1 -- 1 : N
)

-- c) N dest_id - 1 src_id : replace src em.id with null, to be incremented, and update src easybill_id -> dest_id
-- 2 rows
UPDATE bas_firms.easybill_medisoft em2
SET id = NULL 
WHERE id IN (
    WITH cte AS (
        SELECT count(DISTINCT dest.id) dest_cnt, dest.easybill_id , count(DISTINCT src.id) src_cnt FROM bas_firms.easybill_clean ec 
        JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
        JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
        WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
        AND dest.medisoft_id IS NOT NULL
        AND src.medisoft_id IS NOT NULL  
        GROUP BY dest.easybill_id)
    SELECT em.id -- src.id
    FROM cte JOIN bas_firms.easybill_clean ec ON ec.merge_mother = cte.easybill_id 
    JOIN bas_firms.easybill_medisoft em ON em.easybill_id = ec.easybill_id 
    WHERE ec.merge_mother <> ec.easybill_id 
    and dest_cnt > 1 AND src_cnt = 1 -- N : 1
)

-- a,b,c (1): update src em.easybill_id -> dest em.easybill_id (aka dest_id)
-- 5 rows
UPDATE bas_firms.easybill_medisoft em 
SET easybill_id = (SELECT DISTINCT -- in case 2 lines have the same mother
merge_mother FROM bas_firms.easybill_clean ec WHERE ec.easybill_id = em.easybill_id )
WHERE easybill_id IN (
    WITH cte AS (SELECT count(DISTINCT dest.id) dest_cnt, dest.easybill_id , count(DISTINCT src.id) src_cnt FROM bas_firms.easybill_clean ec 
    JOIN bas_firms.easybill_medisoft dest ON dest.easybill_id = ec.merge_mother  
    JOIN bas_firms.easybill_medisoft src ON src.easybill_id = ec.easybill_id 
    WHERE ec.merge_mother IS NOT NULL AND ec.merge_mother <> ec.easybill_id 
    AND dest.medisoft_id IS NOT NULL
    AND src.medisoft_id IS NOT NULL  
    GROUP BY dest.easybill_id)
SELECT DISTINCT em.easybill_id -- src.id
FROM cte JOIN bas_firms.easybill_clean ec ON ec.merge_mother = cte.easybill_id 
JOIN bas_firms.easybill_medisoft em ON em.easybill_id = ec.easybill_id 
WHERE ec.merge_mother <> ec.easybill_id 
)

-- (2) increment null values
-- CREATE SEQUENCE bas_firms.easybill_merge_seq;
-- SET SEQUENCE VALUE TO MAX ID
SELECT setval('bas_firms.easybill_merge_seq', 130007945); 
UPDATE bas_firms.easybill_medisoft em 
SET id = nextval('bas_firms.easybill_merge_seq')::text
WHERE id IS NULL;

-- id = easybill_id mais ils ont + d'un match
SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id = easybill_id
except 
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) = 1 ; 

-- they should not have id = easybill_id
-- we need to increment them 
-- 8 rows
UPDATE bas_firms.easybill_medisoft 
SET id = nextval('bas_firms.easybill_merge_seq')::text
WHERE id = easybill_id
AND easybill_id IN ( SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id = easybill_id
except 
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) = 1 ) ;

-- clean it further
SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE medisoft_id IS NULL 
INTERSECT 
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) > 1;

SELECT * FROM bas_firms.easybill_medisoft em 
WHERE easybill_id IN ('126000005','130000867') ; 

DELETE FROM bas_firms.easybill_medisoft WHERE id IN ('130005353', '130006374') ;

UPDATE bas_firms.easybill_medisoft em SET id = '126000005' WHERE id = '130005352' ; 

--Check if 1E x 1M then Id = Easybill_ID
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) = 1 
EXCEPT 
SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id = easybill_id

SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id = easybill_id
EXCEPT 
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) = 1 

-- Check if 1E x n M then Id <> Easybill_Id
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) > 1 
EXCEPT 
SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id <> easybill_id

SELECT easybill_id FROM bas_firms.easybill_medisoft WHERE id <> easybill_id
EXCEPT
SELECT easybill_id FROM bas_firms.easybill_medisoft 
GROUP BY easybill_id 
HAVING count(*) > 1 
-----------------------------------------------------------------------------------------------------------------

-- EASYBILL_ZOHO
SELECT src.*, dest.*, ec.easybill_id, ec.merge_mother  FROM bas_firms.easybill_zoho src 
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src.easybill_id 
JOIN bas_firms.easybill_zoho dest ON dest.easybill_id = ec.merge_mother 
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id 
--AND coalesce(src.zoho_id,'') <> coalesce(dest.zoho_id,'') ; -- this is a) replace <> with = to have b)

-- 2 cases:
-- a) the easybill src_id is associated a zoho, which is different from the zoho associated to the easybill dest_id
-- -> update zoho.Deals, and replace zoho src with zoho dest, so that opportunities are linked on the dest easybill_id
-- 5 cases
-- b) the easybill src_id is associated to the same zoho as easybill dest_id
-- 14 cases
-- -> no need to update zoho
-- in BOTH cases, we delete the row with src easybill_id from easybill_zoho

-- a) update zoho.Deals, and replace zoho src with zoho dest, so that opportunities are linked on the dest easybill_id
-- 85 rows
UPDATE zoho."Deals" d
SET d."Account_Name" = (
    SELECT dest.zoho_id -- replace zoho src with zoho dest
    FROM bas_firms.easybill_zoho src 
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src.easybill_id 
    JOIN bas_firms.easybill_zoho dest ON dest.easybill_id = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id 
    AND coalesce(src.zoho_id,'') <> coalesce(dest.zoho_id,'')
    AND d."Account_Name" = src.zoho_id)
WHERE d."Account_Name" IN (
    SELECT src2.zoho_id FROM bas_firms.easybill_zoho src2 -- zoho_id associated to easybill src_id
    JOIN bas_firms.easybill_clean ec2 ON ec2.easybill_id = src2.easybill_id 
    WHERE ec2."merge" AND ec2.merge_mother <> ec2.easybill_id
    AND src2.zoho_id IS NOT null)
;

-- both a) and b) delete the row with src easybill_id from easybill_zoho
-- 19
DELETE FROM bas_firms.easybill_zoho 
WHERE easybill_id IN (SELECT src.easybill_id FROM bas_firms.easybill_zoho src 
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src.easybill_id 
JOIN bas_firms.easybill_zoho dest ON dest.easybill_id = ec.merge_mother 
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id )

------------------------------------------------------------------------------------------------
-- FULL_BASIC_CARE
-- BASIC_CARE_DETAILS
-- 1 case
-- we delete the row src easybill_id from both

-- 1 row
DELETE FROM bas_firms.full_basic_care WHERE mother_client_id IN (
SELECT fbc.mother_client_id FROM bas_firms.full_basic_care fbc 
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = fbc.mother_client_id 
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id )
;
-- 1 row
DELETE FROM bas_firms.basic_care_details WHERE mother_client_id IN (
SELECT bcd.mother_client_id FROM bas_firms.basic_care_details bcd 
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = bcd.mother_client_id 
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id )
;
---------------------------------------------------------------------------------------------------------
-- DOCUMENTS
-- we just need to replace src easybill_id with dest easybill_id
-- 190 rows
UPDATE easybill.documents d2 SET "Kontakt: Kundennummer" = (SELECT ec.merge_mother  FROM bas_firms.easybill_clean ec WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
AND ec.easybill_id = d2."Kontakt: Kundennummer")
WHERE "Kontakt: Kundennummer" IN (
SELECT d."Kontakt: Kundennummer" FROM easybill.documents d 
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = d."Kontakt: Kundennummer"
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id )
;

----------------------------------------------------------------------------------------------------------
-- CONTACTS
-- before replacing src easybill_id with dest easybill_id (2)
-- we first replace src firm name, address, postcode and city (1)
-- (1)
-- firm name
UPDATE easybill.contacts c 
SET "Kontakt: Firma" = (SELECT dest."Kontakt: Firma" 
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
    AND c."Kontakt: Kundennummer" = src."Kontakt: Kundennummer" )
WHERE c."Kontakt: Kundennummer" IN (
    SELECT src."Kontakt: Kundennummer"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id) ;

-- address
UPDATE easybill.contacts c 
SET "Kontakt: Straße/Hausnummer" = (SELECT dest."Kontakt: Straße/Hausnummer"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
    AND c."Kontakt: Kundennummer" = src."Kontakt: Kundennummer" )
WHERE c."Kontakt: Kundennummer" IN (
    SELECT src."Kontakt: Kundennummer"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id);

-- post code
UPDATE easybill.contacts c 
SET "Kontakt: Postleitzahl"  = (SELECT dest."Kontakt: Postleitzahl"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
    AND c."Kontakt: Kundennummer" = src."Kontakt: Kundennummer" )
WHERE c."Kontakt: Kundennummer" IN (
    SELECT src."Kontakt: Kundennummer"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id);

-- city
UPDATE easybill.contacts c 
SET "Kontakt: Ort"   = (SELECT dest."Kontakt: Ort"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
    AND c."Kontakt: Kundennummer" = src."Kontakt: Kundennummer" )
WHERE c."Kontakt: Kundennummer" IN (
    SELECT src."Kontakt: Kundennummer"
    FROM easybill.contacts src
    JOIN bas_firms.easybill_clean ec ON ec.easybill_id = src."Kontakt: Kundennummer"
    JOIN easybill.contacts dest ON dest."Kontakt: Kundennummer" = ec.merge_mother 
    WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id);

-- (2) replace src easybill_id with dest easybill_id
UPDATE easybill.contacts c2 SET "Kontakt: Kundennummer" = (SELECT ec.easybill_id FROM bas_firms.easybill_clean ec WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id
AND ec.easybill_id = c2."Kontakt: Kundennummer")
WHERE "Kontakt: Kundennummer" IN (
SELECT c."Kontakt: Kundennummer" FROM easybill.contacts c
JOIN bas_firms.easybill_clean ec ON ec.easybill_id = c."Kontakt: Kundennummer"
WHERE ec."merge" AND ec.merge_mother <> ec.easybill_id )
;
------------------------------------------------------------------------------------------------------------
