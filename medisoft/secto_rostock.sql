-- firm_doctor -> doctor_occurrences -> select
-- last_visit_date --------------------|
-- number of employees -----------------|
WITH firm_doctor AS (
    SELECT b.ebetrieb_id , be.vollstaend_name , count(*) nb FROM table_untersuchungen u 
    JOIN table_beschaeftigte b ON b.rec_id = u.besch_id 
    JOIN table_firmenstruktur f ON f.rec_id = b.ebetrieb_id 
    JOIN table_benutzer be ON be.rec_id = u.arzt_stempel 
    WHERE lower(f.mandant) = 'rostock'
    GROUP BY b.ebetrieb_id , be.vollstaend_name
), doctor_occurrences AS (
    SELECT ebetrieb_id, vollstaend_name || ' (' ||  nb || ')' physician, 
    row_number() over(PARTITION BY ebetrieb_id ORDER BY nb DESC, vollstaend_name) rang
    FROM firm_doctor fd 
), 
last_visit_date AS (
    SELECT b.ebetrieb_id, max(u.u_datum::date) last_date
    FROM medisoft.table_beschaeftigte b 
    JOIN medisoft.table_untersuchungen u ON u.besch_id = b.rec_id
    GROUP BY b.ebetrieb_id
), number_of_employees AS (
SELECT ebetrieb_id, count(*) nb FROM table_beschaeftigte  
GROUP BY ebetrieb_id 
)
SELECT em.id padoa_id, f.rec_id medisoft_id , f.kuerzel, lvd.last_date last_visit_date, ne.nb number_employee,d1.physician physician_1, d2.physician physician_2  FROM medisoft.table_firmenstruktur f 
LEFT JOIN last_visit_date lvd ON lvd.ebetrieb_id = f.rec_id 
LEFT JOIN number_of_employees ne ON ne.ebetrieb_id = f.rec_id 
LEFT JOIN bas_firms.easybill_medisoft em ON em.medisoft_id = f.rec_id 
LEFT JOIN bas_firms.full_basic_care fbc ON fbc.mother_client_id = em.easybill_id  -- on va les enlever, cf. where
LEFT JOIN doctor_occurrences d1 ON d1.ebetrieb_id = f.rec_id AND d1.rang=1 -- premier médecin le plus fréquent
LEFT JOIN doctor_occurrences d2 ON d2.ebetrieb_id = f.rec_id AND d2.rang=2 -- deuxième médecin le plus fréquent
LEFT JOIN bas_firms.cleaned_medisoft cm ON cm.medisoft_id = f.rec_id -- to keep only clean medisoft
WHERE last_date > '2023-01-01'::date -- firm active
AND lower(f.mandant) = 'rostock'
AND fbc.mother_client_id IS NULL -- quand c'est une firm du dashboard, on a déjà l'équipe médicale, donc on les exclut
AND ((cm.migrate_as_inactive = FALSE AND cm.no_migration = FALSE) -- les inactives et no_migration ne nous intéressent pas
    OR cm.medisoft_id IS NULL ) -- OR pour avoir les cas où medisoft id n'est pas dans cleaned_medisoft
ORDER BY ne.nb DESC;