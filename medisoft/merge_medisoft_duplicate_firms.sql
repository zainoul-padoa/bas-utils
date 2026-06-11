-- Here is the DAG between CTEs
-- duplicates ---------->  list_duplicates -> merge_medisoft
-- number_of_employees ___|
-- list of fields used to merge : kuerzel, coalesce(name,kuezel), strasse, plz, ort, lower(mandant)
-- we merge on the medisoft_id which has the most employees
-- i put coalesce(name, kuerzel) because name is sometimes null, ex. bipG Hannover's name is once null and another time not null, creating two groups whereas it is the same
-- coalesce are needed in joins for address and nullable fields
CREATE TABLE bas_firms.merge_medisoft AS 
WITH duplicates AS (
SELECT tf.kuerzel , COALESCE(tf.name, tf.kuerzel) AS name , tf.strasse , tf.plz , ort, lower(mandant) AS mandant, count(*) FROM medisoft.table_firmenstruktur tf 
WHERE tf.kuerzel IS NOT NULL OR tf."name" IS NOT NULL -- null names: no interest for us
GROUP BY  tf.kuerzel , COALESCE(tf.name, tf.kuerzel), tf.strasse , tf.plz , ort, lower(mandant)
HAVING count(*) > 1 -- this filter retrieves duplicates only
) , 
number_of_employees AS (
SELECT ebetrieb_id, count(*) nb FROM table_beschaeftigte  
GROUP BY ebetrieb_id 
), 
list_duplicates AS (
SELECT f.rec_id , COALESCE(f."name", f.kuerzel ) AS name , f.kuerzel , f.strasse, f.plz , f.ort , du.mandant , ne.nb ,
row_number() over(PARTITION BY f.kuerzel , COALESCE(f."name", f.kuerzel ) , f.strasse , f.plz , f.ort, lower(f.mandant) ORDER BY ne.nb desc) AS rang  
FROM table_firmenstruktur f 
JOIN duplicates du ON COALESCE(du."name",du.kuerzel,'') = COALESCE(f.name,f.kuerzel ,'') AND COALESCE(du.kuerzel,'') = COALESCE(f.kuerzel,'') AND COALESCE(du.strasse,'') = COALESCE(f.strasse,'') AND COALESCE(du.plz,'') = COALESCE(f.plz,'') AND COALESCE(du.ort,'') = COALESCE(f.ort,'') AND du.mandant = lower(f.mandant)
LEFT JOIN number_of_employees ne ON ne.ebetrieb_id = f.rec_id 
)
SELECT src.rec_id src_id, dest.rec_id dest_id FROM list_duplicates dest 
JOIN list_duplicates src ON src.name = dest.name AND src.kuerzel = dest.kuerzel AND COALESCE(src.strasse,'') = COALESCE(dest.strasse,'') AND COALESCE(src.plz,'') = COALESCE(dest.plz,'') AND COALESCE(src.ort,'') = COALESCE(dest.ort,'') AND src.mandant = dest.mandant 
WHERE dest.rang = 1 AND src.rang > 1
ORDER BY dest.rec_id;

-- SAVES
CREATE TABLE medisoft.table_firmenstruktur_save AS 
SELECT * FROM medisoft.table_firmenstruktur  ;

CREATE TABLE bas_firms.easybill_medisoft_save AS 
SELECT * FROM bas_firms.easybill_medisoft ;

CREATE TABLE bas_firms.cleaned_medisoft_save AS 
SELECT * FROM bas_firms.cleaned_medisoft;


DELETE FROM medisoft.table_firmenstruktur  
WHERE rec_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

DELETE FROM bas_firms.cleaned_medisoft
WHERE medisoft_id IN (SELECT src_id FROM bas_firms.merge_medisoft);


-- to avoid losing data about Easybill x Medisoft match
UPDATE bas_firms.easybill_medisoft em 
SET medisoft_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = em.medisoft_id)
WHERE em.medisoft_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

-- remove duplicates from easybill_medisoft
DELETE FROM bas_firms.easybill_medisoft em 
USING bas_firms.easybill_medisoft em2 
WHERE em.id > em2.id 
AND em.easybill_id = em2.easybill_id AND em.medisoft_id = em2.medisoft_id

--TODO: 
-- update em.id where there is only 1 row for an Easybill_ID


-- replace in each relevant table containing ebetrieb_id 

UPDATE medisoft.table_untersuchungen u
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = u.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

UPDATE medisoft.table_beschaeftigte b 
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = b.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

UPDATE medisoft.table_kartei t
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = t.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

UPDATE medisoft.table_ap_historie t
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = t.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

UPDATE medisoft.table_erbrachte_werte t
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = t.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);

UPDATE medisoft.table_impfungen t
SET ebetrieb_id = (SELECT mm.dest_id FROM bas_firms.merge_medisoft mm WHERE mm.src_id = t.ebetrieb_id)
WHERE ebetrieb_id IN (SELECT src_id FROM bas_firms.merge_medisoft);
