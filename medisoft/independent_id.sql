---------- INDEPENDENT ID ------------
-- get max id
SELECT * FROM bas_firms.easybill_medisoft em
ORDER BY id DESC;

-- set sequence to that id
CREATE SEQUENCE bas_firms.medisoft_independent;
SELECT setval('bas_firms.medisoft_independent', 130007950); 

-- add id to easybill_medisoft
INSERT INTO bas_firms.easybill_medisoft (medisoft_id, id)
SELECT b.rec_id , nextval('bas_firms.medisoft_independent') FROM medisoft.table_beschaeftigte b 
LEFT JOIN medisoft.table_firmenstruktur f ON f.rec_id = b.ebetrieb_id 
WHERE lower(f.kuerzel) like '%selbstzahler%'
AND b.rec_id NOT IN (SELECT medisoft_id FROM bas_firms.easybill_medisoft) -- if it is already in easybill_medisoft, don't insert it again please!

-- employee without company
---- GENERATE INDEPENT FIRM SOURCE ID
-- get max id
SELECT * FROM bas_firms.easybill_medisoft em
ORDER BY id DESC;

-- set sequence to that id
SELECT setval('bas_firms.medisoft_independent', 130016749); 

-- add id to easybill_medisoft
INSERT INTO bas_firms.easybill_medisoft (medisoft_id, id)
SELECT b.rec_id , nextval('bas_firms.medisoft_independent') FROM medisoft.table_beschaeftigte b 
WHERE inactive AND ebetrieb_id IS NULL
AND b.rec_id NOT IN (SELECT medisoft_id FROM bas_firms.easybill_medisoft) -- if it is already in easybill_medisoft, don't insert it again please!
