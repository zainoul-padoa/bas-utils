--- PRE-CLEAN EASYBILL
ALTER TABLE easybill.documents 
ALTER COLUMN "Kontakt: Kundennummer" TYPE text;


SELECT 'UPDATE easybill.contacts SET "'||column_name||'" = null WHERE "'||column_name||'" = '''';'
FROM information_schema."columns" 
WHERE table_name = 'contacts' AND column_name IN ('Kontakt: Straße/Hausnummer', 'Kontakt: Name',  
'Kontakt: Vorname', 'Kontakt: E-Mail', 'Kontakt: Mobiltelefon', 'Kontakt: Telefon 1','Kontakt: Telefon 2','Kontakt: Weitere E-Mails');

UPDATE easybill.contacts SET "Kontakt: Vorname" = null WHERE "Kontakt: Vorname" = '';
UPDATE easybill.contacts SET "Kontakt: Name" = null WHERE "Kontakt: Name" = '';
UPDATE easybill.contacts SET "Kontakt: Straße/Hausnummer" = null WHERE "Kontakt: Straße/Hausnummer" = '';
UPDATE easybill.contacts SET "Kontakt: Telefon 1" = null WHERE "Kontakt: Telefon 1" = '';
UPDATE easybill.contacts SET "Kontakt: Telefon 2" = null WHERE "Kontakt: Telefon 2" = '';
UPDATE easybill.contacts SET "Kontakt: Mobiltelefon" = null WHERE "Kontakt: Mobiltelefon" = '';
UPDATE easybill.contacts SET "Kontakt: E-Mail" = null WHERE "Kontakt: E-Mail" = '';
UPDATE easybill.contacts SET "Kontakt: Weitere E-Mails" = null WHERE "Kontakt: Weitere E-Mails" = '';

-- update basic_care child id for deduplication
update bas_firms.full_basic_care fbc
set child_client_id = sub.suffixed_id
from (
  select
    ctid,
    case
      when count(*) over (partition by mother_client_id) > 1
      then mother_client_id || row_number() over (partition by mother_client_id)
      else mother_client_id
    end as suffixed_id
  from bas_firms.full_basic_care
) sub
where fbc.ctid = sub.ctid;

