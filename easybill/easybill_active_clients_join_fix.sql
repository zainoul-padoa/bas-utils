-- Join: exact name match first, cleaned name only when no exact match exists for this eb row.
-- Also fixes: trailing comma after zoho_firm.

with cleaned_easybill_active_clients as (
    select
        *,
        clean_account_name(Firma) as clean_easybill_firm
    from easybill_active_clients
), cleaned_zoho_accounts as (
    select
        *,
        clean_account_name("Accounts Name") as clean_zoho_firm
    from zoho_accounts
)
select
    eb.Kundennummer,
    eb.Firma as easybill_firm,
    z."Accounts Name" as zoho_firm
from cleaned_easybill_active_clients eb
join cleaned_zoho_accounts z
    on eb.Firma = z."Accounts Name"
    or (
        eb.clean_easybill_firm = z.clean_zoho_firm
        and not exists (
            select 1 from cleaned_zoho_accounts z2
            where z2."Accounts Name" = eb.Firma
        )
    )
where eb.Kundennummer not in (select Kundennummer from easybill_zoho_kundennummer_match)
and eb.Kundennummer = '130000537'
