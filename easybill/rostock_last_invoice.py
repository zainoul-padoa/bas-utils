"""
For each Rostock client in the Wochenliste, list the AM-R / AS-R line items
of their LAST invoice.

"Last invoice" = the most recent NON-FUTURE document (by Dokument: Datum,
falling back to Leistungsdatum von for recurring invoices that have no date)
that actually contains an AM-R or AS-R line. Documents dated after today are
excluded, which drops the future-period recurring invoices.

is_pauschal: flat-fee line (Posten: Anzahl == '1') vs. per-hour quantity.

Output: easybill/output/rostock_last_invoice_am_as.csv
"""

from pathlib import Path

from merge_tables.db.connection import connect_to_postgres_via_duckdb

WOCHENLISTE_XLSM = "/Users/adrienblanquer/Downloads/PADOA EXPORT_2026_KW23_Wochenliste_20260604_V1.xlsm"
OUTPUT_PATH = Path(__file__).parent / "output" / "rostock_last_invoice_am_as.csv"
OUTPUT_PATH.parent.mkdir(exist_ok=True)

# explicit list of Wochenliste "ID Nummer" to report on (replaces the Standort filter)
CLIENT_IDS = [
    130000200, 130000178, 130000660, 100020038, 100020065, 130001591, 130000505,
    1300002701, 130002204, 130000994, 101000034, 130002299, 130000282, 130002325,
    130002318, 130000761, 130000582, 130002085, 130001589, 104000039, 103000021,
    130001230, 130000813, 104000051, 130002375, 130001576, 130001050, 130001272,
    130002326, 130002317, 108040047, 108040050, 108040056, 130000626, 108040045,
    130002331, 130001097, 109020025, 130000869, 109020021, 130002389, 130002041,
    130000858, 130000908, 130000907, 130000906, 130002418, 130000327, 130002322,
    130002323, 130002320, 130000878, 111000022, 130001330, 111001021, 130001568,
    130001844, 1130100175, 130000617, 115000020, 115000012, 115000009, 130000853,
    130001699, 130000947, 130001068, 130001583, 130000581, 130001169, 130002090,
    130002293, 130000189, 130001329, 130002309, 130000886, 130001200, 130001582,
    130002049, 130002003, 120000030, 120000042, 130000707, 130000847, 130000274,
    130000601, 130000158, 130001819, 130001570, 130000186, 130000543, 130000995,
    130002376,
]
ids_sql = ", ".join(str(i) for i in CLIENT_IDS)

duck = connect_to_postgres_via_duckdb()

# --- load the Wochenliste -----------------------------------------------------
duck.execute("install excel; load excel;")
duck.execute(
    f"""
    create or replace table wochenliste as
    select * replace("ID Nummer"::int64 as "ID Nummer")
    from read_xlsx('{WOCHENLISTE_XLSM}', sheet='Kunden', header=True, range='B5:D998')
    """
)

# --- pull the easybill invoice line items ------------------------------------
duck.execute(
    """
    create or replace table easybill_documents as
    select
        "Kontakt: Kundennummer"::varchar as id_easybill,
        "Dokument: ID",
        "Dokument: Typ",
        "Dokument: Datum",
        "Dokument: Leistungsdatum von",
        "Dokument: Leistungsdatum bis",
        "Posten: Artikelnummer",
        "Posten: Artikelbeschreibung",
        "Posten: Typ",
        "Posten: Nettobetrag",
        "Posten: Bruttobetrag",
        "Posten: Anzahl"
    from pg.easybill.documents
    """
)

# --- last AM-R/AS-R invoice per Rostock client -------------------------------
result = duck.sql(
    f"""
    with requested(id_nummer) as (
        select unnest([{ids_sql}])
    ),
    selected_clients as (
        -- driven by the requested ID list; Wochenliste / easybill contacts only
        -- supply the company name (an ID absent from the Wochenliste still appears)
        select distinct on (left(r.id_nummer::varchar, 9))
            left(r.id_nummer::varchar, 9) as id_easybill,
            coalesce(w.Firmenname, c."Kontakt: Firma") as Firmenname,
            r.id_nummer as "ID Nummer",
            w.Standort
        from requested r
        left join wochenliste w
            on w."ID Nummer" = r.id_nummer
        left join pg.easybill.contacts c
            on c."Kontakt: Kundennummer" = left(r.id_nummer::varchar, 9)
        order by left(r.id_nummer::varchar, 9), r.id_nummer
    ),
    am_as_lines as (
        -- every AM-R / AS-R line, with a sortable date (recurring -> Leistungsdatum von)
        select
            *,
            coalesce("Dokument: Datum", "Dokument: Leistungsdatum von") as sort_date,
            "Posten: Anzahl" == '1' as is_pauschal
        from easybill_documents
        where "Posten: Artikelnummer" in ('AM-R', 'AS-R')
            -- no invoices in the future
            and coalesce("Dokument: Datum", "Dokument: Leistungsdatum von")::date <= current_date
            -- no negative prices (e.g. Gutschrift / credit notes)
            and not starts_with("Posten: Nettobetrag", '-')
    ),
    last_invoice as (
        -- the most recent document (per client) that carries an AM-R/AS-R line
        select distinct on (id_easybill)
            id_easybill,
            "Dokument: ID" as last_doc_id
        from am_as_lines
        order by id_easybill, sort_date desc
    )
    select
        w.Firmenname,
        w."ID Nummer",
        w.Standort,
        l.id_easybill,
        l."Dokument: ID",
        l."Dokument: Typ",
        l."Dokument: Datum",
        l.sort_date,
        l."Posten: Artikelnummer",
        l."Posten: Artikelbeschreibung",
        l."Posten: Anzahl",
        l.is_pauschal,
        l."Posten: Nettobetrag",
        l."Posten: Bruttobetrag",
        round(
            replace(l."Posten: Nettobetrag", ',', '.')::double
            / nullif(replace(l."Posten: Anzahl", ',', '.')::double, 0),
            2
        ) as hourly_net_price,
        -- reference price: 5000 flat for pauschal, 300/hour for non-pauschal
        case when l.is_pauschal then 5000 else 300 end as ref_price,
        -- variation vs reference: positive = increase, negative = reduction
        round((hourly_net_price - ref_price) / ref_price * 100, 2) as variation_pct
    from selected_clients w
    join last_invoice li using (id_easybill)
    join am_as_lines l
        on l.id_easybill = li.id_easybill
        and l."Dokument: ID" = li.last_doc_id
    order by w.Firmenname, l."Posten: Artikelnummer"
    """
)

result.to_csv(str(OUTPUT_PATH))

# --- quick summary -----------------------------------------------------------
print(f"\n✓ wrote {OUTPUT_PATH}")
duck.sql(
    f"""
    select
        count(*)                          as nb_line_items,
        count(distinct id_easybill)       as nb_clients,
        count(distinct "Dokument: ID")    as nb_invoices
    from read_csv('{OUTPUT_PATH}')
    """
).show()

print(f"\nRequested IDs: {len(CLIENT_IDS)}")

# requested IDs not present in the Wochenliste at all
not_in_wochenliste = duck.sql(
    f"""
    with requested(id) as (select unnest([{ids_sql}]))
    select id from requested
    where id not in (select "ID Nummer" from wochenliste where "ID Nummer" is not null)
    order by id
    """
).fetchall()
print(f"Not found in Wochenliste ({len(not_in_wochenliste)}): "
      f"{[r[0] for r in not_in_wochenliste]}")

# requested IDs present in Wochenliste but with no AM-R/AS-R invoice in the output
no_invoice = duck.sql(
    f"""
    with requested(id) as (select unnest([{ids_sql}]))
    select id from requested
    where left(id::varchar, 9) not in (select distinct id_easybill::varchar from read_csv('{OUTPUT_PATH}'))
    order by id
    """
).fetchall()
print(f"Requested but no AM-R/AS-R invoice ({len(no_invoice)}): "
      f"{[r[0] for r in no_invoice]}")
