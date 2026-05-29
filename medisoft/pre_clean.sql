-- PRE-CLEAN MEDISOFT

----- Manage employee without company
-- when visit has a company, we take the most recent one as company
-- and employee is flagged as inactive
-- see https://docs.google.com/spreadsheets/d/1tNLxvFFcNEbTEsns9Gnjfl06g1A919l3WAggbHWA7JY/edit?pli=1&gid=390814330#gid=390814330
CREATE TABLE medisoft.table_beschaeftigte_2 AS 
SELECT * FROM medisoft.table_beschaeftigte;

ALTER TABLE medisoft.table_beschaeftigte_2
ADD inactive boolean;

UPDATE medisoft.table_beschaeftigte_2 b2
SET ebetrieb_id = (WITH employer_from_visit as (
    select
        u.besch_id ,
        u.ebetrieb_id,
        row_number() over(partition by u.besch_id order by nullif(u.u_datum, '0000-00-00T00:00:00')::date desc nulls last) as rang
    from
        medisoft.table_untersuchungen u
    where
        u.ebetrieb_id is not null
        and u.u_datum is not null
    )
SELECT efm.ebetrieb_id
    FROM medisoft.table_beschaeftigte b
    LEFT JOIN employer_from_visit efm ON efm.besch_id = b.rec_id AND rang=1
    WHERE b.rec_id = b2.rec_id
    AND b.ebetrieb_id IS NULL -- no company
    AND b.identifikation NOT IN ('Mustermann Max  01.01.2000', 'Maxmustermann Alexander  06.08.1985','Mustermensch Max  20.01.1983','Musterfrau Lisa  01.01.1957','Musterfrau Alina  01.01.1999','Mustermuster Mustermax  12.12.1970','Mustertesttest Max  12.04.1989','Mustermann Maximilian  01.01.1990','Test Mustermann  01.02.2001','Mustermann Muster  01.11.1988','Test Mustermann  01.02.1989','Muster Nadja  19.03.1996','Musterneu Muster  01.01.2000','Mustermann Dennis  21.11.1995','Mustermann Otto  11.03.1980','Test Muster  12.11.1980','Muster Test  01.01.1980','Muster Musterfrau  01.01.2000','Mustermann Maxime  01.01.1990','Nikolaus Sankt  24.12.2000','Nikolaus Sankt  17.08.1999','Nikolaus Sankt  24.12.2000','Nikolaus Sankt  17.08.1999','Testi Wilhelmine-Margarete  01.01.1975','Test Monika  27.01.1999','Gründler Testi Michael  01.01.1972','Test Testi  23.09.1978','Test Testine  01.01.1912','Test Test  01.01.1975','Test Testine  01.01.1990','Test Testmann  12.12.1980','Test Mustermann  01.02.2001','Test Testa  18.01.1958','Aussendienst Test  19.04.1988','Testmann Testus  03.01.1990','Test Mustermann  01.02.1989','Test Marek  01.01.1990','TEST Testus  23.07.1999','Test Köln  15.02.1998','Test TEST  21.10.1999','Test Gündag Gizem  29.09.1997','Test Frankfurt  01.01.1999','Test Jürgen  01.01.1956','Testfrau Testine  23.09.1978','TestGündag Test Gizem  29.09.1997','Test Gina  12.12.2000','Test Eirini  25.02.2006','Test Steffi  01.01.2000','Test Anna  18.10.1999','Test München  01.01.2000','Test München  18.01.1995','Test Hamburg  01.01.2000','Test Britta  01.01.2000','Test Tanja  01.01.2000','Test Muster  12.11.1980','Test Testi  01.01.1993','max Test  01.01.2000','Test Basis  01.01.2000','Test Julia  01.01.2000','Test Test  01.01.2000','Muster Test  01.01.1980','Test uschi  05.05.1962','Testii Tester  10.02.1995','Test Testamann  03.10.2000','Test Basis  01.01.1999','Test Taxi  01.01.1990','Test Tanja Nummer 2  01.01.1995','Test Martina  01.01.2000','Dummy Doris  01.01.1966')
    AND familienname IS NOT NULL 
    AND familienname NOT IN ('x','xx','xxx') AND vorname NOT IN ('x','xx','xxx') 
), 
inactive = TRUE
WHERE ebetrieb_id IS null
    AND b2.identifikation NOT IN ('Mustermann Max  01.01.2000', 'Maxmustermann Alexander  06.08.1985','Mustermensch Max  20.01.1983','Musterfrau Lisa  01.01.1957','Musterfrau Alina  01.01.1999','Mustermuster Mustermax  12.12.1970','Mustertesttest Max  12.04.1989','Mustermann Maximilian  01.01.1990','Test Mustermann  01.02.2001','Mustermann Muster  01.11.1988','Test Mustermann  01.02.1989','Muster Nadja  19.03.1996','Musterneu Muster  01.01.2000','Mustermann Dennis  21.11.1995','Mustermann Otto  11.03.1980','Test Muster  12.11.1980','Muster Test  01.01.1980','Muster Musterfrau  01.01.2000','Mustermann Maxime  01.01.1990','Nikolaus Sankt  24.12.2000','Nikolaus Sankt  17.08.1999','Nikolaus Sankt  24.12.2000','Nikolaus Sankt  17.08.1999','Testi Wilhelmine-Margarete  01.01.1975','Test Monika  27.01.1999','Gründler Testi Michael  01.01.1972','Test Testi  23.09.1978','Test Testine  01.01.1912','Test Test  01.01.1975','Test Testine  01.01.1990','Test Testmann  12.12.1980','Test Mustermann  01.02.2001','Test Testa  18.01.1958','Aussendienst Test  19.04.1988','Testmann Testus  03.01.1990','Test Mustermann  01.02.1989','Test Marek  01.01.1990','TEST Testus  23.07.1999','Test Köln  15.02.1998','Test TEST  21.10.1999','Test Gündag Gizem  29.09.1997','Test Frankfurt  01.01.1999','Test Jürgen  01.01.1956','Testfrau Testine  23.09.1978','TestGündag Test Gizem  29.09.1997','Test Gina  12.12.2000','Test Eirini  25.02.2006','Test Steffi  01.01.2000','Test Anna  18.10.1999','Test München  01.01.2000','Test München  18.01.1995','Test Hamburg  01.01.2000','Test Britta  01.01.2000','Test Tanja  01.01.2000','Test Muster  12.11.1980','Test Testi  01.01.1993','max Test  01.01.2000','Test Basis  01.01.2000','Test Julia  01.01.2000','Test Test  01.01.2000','Muster Test  01.01.1980','Test uschi  05.05.1962','Testii Tester  10.02.1995','Test Testamann  03.10.2000','Test Basis  01.01.1999','Test Taxi  01.01.1990','Test Tanja Nummer 2  01.01.1995','Test Martina  01.01.2000','Dummy Doris  01.01.1966')
    AND familienname IS NOT NULL 
    AND familienname NOT IN ('x','xx','xxx') AND vorname NOT IN ('x','xx','xxx') 
; 

ALTER TABLE medisoft.table_beschaeftigte 
RENAME TO table_beschaeftigte_save;

ALTER TABLE medisoft.table_beschaeftigte_2
RENAME TO table_beschaeftigte;
-- end manage employee without company
--------------------------------------------------------------