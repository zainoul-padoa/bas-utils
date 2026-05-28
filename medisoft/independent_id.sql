---------- INDEPENDENT ID ------------
-- get max id
SELECT * FROM bas_firms.easybill_medisoft em
ORDER BY id DESC;

-- set sequence to that id
CREATE SEQUENCE bas_firms.medisoft_independent;
SELECT setval('bas_firms.medisoft_independent', 130007950); 

-- add id to easybill_medisoft
INSERT INTO bas_firms.easybill_medisoft (medisoft_id, id)
SELECT b.rec_id , nextval('bas_firms.medisoft_independent') FROM table_beschaeftigte b 
LEFT JOIN table_firmenstruktur f ON f.rec_id = b.ebetrieb_id 
WHERE lower(f.kuerzel) like '%selbstzahler%';
