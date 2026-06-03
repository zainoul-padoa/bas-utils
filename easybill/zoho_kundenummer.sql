INSERT INTO bas_firms.easybill_zoho(zoho_id, easybill_id)
SELECT a."Id" zoho_id, a."Kundenummer" easybill_id FROM zoho."Accounts" a 
WHERE a."Kundenummer" NOT IN (SELECT easybill_id FROM bas_firms.easybill_zoho ez WHERE ez.zoho_id IS NOT NULL AND easybill_id IS NOT null )
;