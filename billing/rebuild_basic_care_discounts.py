"""
Regenerate basic_care_discounts.csv from the easybill database.

Pipeline:
  1. Client roster  -> a single-column CSV of easybill "ID Nummer" (CLIENT_ID_LIST).
  2. all_clients    -> for each client, the most recent NON-FUTURE, non-credit
                       invoice that carries AM-R / AS-R lines, pulled fresh from
                       pg.easybill.documents (same "last invoice" recipe as
                       easybill/rostock_last_invoice.py). Company name + Standort
                       come from basic_care/full_basic_care.csv, falling back to
                       the easybill Kontakt: Firma. This reproduces the
                       "All clients" Google Sheet (1Rz_dGw4...), 19 columns.
  3. discounts      -> join the padoa reference-price sheet + employee counts
                       (full_basic_care) and compute the % adjustment per client
                       (pricing SQL lifted verbatim from basic_care_prices.ipynb).

Outputs:
  - easybill/output/all_clients_rebuilt.csv   (refreshed "All clients" sheet data)
  - billing/basic_care_discounts.csv          (final deliverable)

Run from the repo root:  uv run python billing/rebuild_basic_care_discounts.py
"""

from pathlib import Path

from merge_tables.db.connection import connect_to_postgres_via_duckdb

ROOT = Path(__file__).resolve().parent.parent

# --- inputs ------------------------------------------------------------------
CLIENT_ID_LIST = Path("/Users/adrienblanquer/Desktop/full_basic_care_202607061739.csv")
FULL_BASIC_CARE = ROOT / "basic_care" / "full_basic_care.csv"
CREDS = ROOT / "billing" / "config" / "gsheet-creds.json"
PADOA_SHEET_ID = "140vMgvckcXRfl13sVGedGWV740JZw-R9QlHbfpJXczE"
PADOA_TAB = "1. Grundbetreuung Updated"

# --- outputs -----------------------------------------------------------------
ALL_CLIENTS_OUT = ROOT / "easybill" / "output" / "all_clients_rebuilt.csv"
DISCOUNTS_OUT = ROOT / "billing" / "basic_care_discounts.csv"
ALL_CLIENTS_OUT.parent.mkdir(exist_ok=True)


def main() -> None:
    duck = connect_to_postgres_via_duckdb()

    # 1. client roster (single-column list of easybill IDs) -------------------
    id_col = duck.sql(
        f"describe (select * from read_csv('{CLIENT_ID_LIST}', all_varchar=true))"
    ).fetchall()[0][0]
    duck.execute(
        f"""
        create or replace table clients as
        select distinct
            "{id_col}"::varchar          as id_nummer,
            left("{id_col}"::varchar, 9) as id_easybill
        from read_csv('{CLIENT_ID_LIST}', all_varchar=true)
        where "{id_col}" is not null
        """
    )

    # names + Standort per mother client (easybill has no Padoa Standort) ------
    duck.execute(
        f"""
        create or replace table fbc as
        select distinct on (left(mother_client_id::varchar, 9))
            left(mother_client_id::varchar, 9) as id_easybill, firm_name, Standort
        from read_csv('{FULL_BASIC_CARE}')
        """
    )

    # 2. rebuild the "All clients" sheet from fresh invoice data --------------
    duck.execute(
        """
        create or replace table easybill_documents as
        select
            "Kontakt: Kundennummer"::varchar as id_easybill,
            "Kontakt: Firma"                 as kontakt_firma,
            "Dokument: ID", "Dokument: Typ", "Dokument: Datum",
            "Dokument: Leistungsdatum Datum", "Dokument: Leistungsdatum von",
            "Dokument: Leistungsdatum bis", "Dokument: Leistungsdatum Benutzerdefiniert",
            "Posten: Artikelnummer", "Posten: Artikelbeschreibung", "Posten: Typ",
            "Posten: Nettobetrag", "Posten: Bruttobetrag", "Posten: Anzahl"
        from pg.easybill.documents
        """
    )

    duck.execute(
        r"""
        create or replace table all_clients_rebuilt as
        with am_as_lines as (
            -- every AM-R / AS-R line with a real sortable date; recurring invoices
            -- fall back to Leistungsdatum von. Dokument: Datum is DD.MM.YYYY text.
            select *,
                try_strptime(coalesce("Dokument: Datum", "Dokument: Leistungsdatum von"),
                             '%d.%m.%Y') as sort_date
            from easybill_documents
            where "Posten: Artikelnummer" in ('AM-R', 'AS-R')
                and try_strptime(coalesce("Dokument: Datum", "Dokument: Leistungsdatum von"),
                                 '%d.%m.%Y') <= current_date      -- drop future recurring periods
                and not starts_with("Posten: Nettobetrag", '-')   -- drop credit notes
        ),
        last_invoice as (        -- the single most recent AM/AS invoice per client
            select distinct on (id_easybill) id_easybill, "Dokument: ID" as last_doc_id
            from am_as_lines
            order by id_easybill, sort_date desc
        )
        select
            c.id_easybill,
            coalesce(fbc.firm_name, l.kontakt_firma)  as "Firmenname",
            c.id_nummer                               as "ID Nummer",
            fbc.Standort                              as "Standort",
            l."Dokument: ID", l."Dokument: Typ", l."Dokument: Datum",
            l."Dokument: Leistungsdatum Datum", l."Dokument: Leistungsdatum von",
            l."Dokument: Leistungsdatum bis", l."Dokument: Leistungsdatum Benutzerdefiniert",
            l."Posten: Artikelnummer", l."Posten: Artikelbeschreibung", l."Posten: Typ",
            l."Posten: Nettobetrag", l."Posten: Bruttobetrag", l."Posten: Anzahl",
            (l."Posten: Anzahl" = '1')                as is_pauschal,
            round(
                try_cast(replace(regexp_replace(l."Posten: Nettobetrag", '[^0-9,]', '', 'g'), ',', '.') as double)
                / nullif(try_cast(replace(l."Posten: Anzahl", ',', '.') as double), 0)
            )::bigint                                 as "hourly prices"
        from clients c
        left join fbc        on fbc.id_easybill = c.id_easybill
        join last_invoice li on li.id_easybill = c.id_easybill
        join am_as_lines l   on l.id_easybill = li.id_easybill
                            and l."Dokument: ID" = li.last_doc_id
        order by c.id_easybill, l."Posten: Artikelnummer"
        """
    )
    duck.sql("select * from all_clients_rebuilt").to_csv(str(ALL_CLIENTS_OUT))

    n_clients = duck.sql("select count(*) from clients").fetchone()[0]
    n_with = duck.sql("select count(distinct id_easybill) from all_clients_rebuilt").fetchone()[0]
    dropped = [
        r[0]
        for r in duck.sql(
            "select id_nummer from clients where id_easybill not in "
            "(select distinct id_easybill from all_clients_rebuilt) order by 1"
        ).fetchall()
    ]
    print(f"✓ all_clients_rebuilt.csv — {n_with}/{n_clients} clients have a usable AM-R/AS-R invoice")
    if dropped:
        print(f"  {len(dropped)} clients with no usable invoice (excluded): {dropped}")

    # 3. reference prices + employee counts -> discounts ----------------------
    duck.execute("INSTALL gsheets FROM community; LOAD gsheets;")
    duck.execute(
        f"CREATE OR REPLACE SECRET gsheet_sa (TYPE gsheet, PROVIDER key_file, "
        f"FILEPATH '{CREDS.resolve()}');"
    )
    duck.sql(
        f"""create or replace table padoa_prices as
        SELECT * FROM read_gsheet('{PADOA_SHEET_ID}', sheet='{PADOA_TAB}',
                                  all_varchar=true, range='A3:N')"""
    )
    duck.sql(f"create or replace table full_basic_care as select * from read_csv('{FULL_BASIC_CARE}')")

    # pricing SQL lifted verbatim from basic_care_prices.ipynb (is_pauschal is a
    # native boolean here, so `bcp.is_pauschal` is used directly).
    duck.sql(
        r"""
        create or replace table discounts as
        with padoa_var as (
            select trim("WZ Group") as bg, trim("Angebot") as angebot,
                regexp_extract("Mitarbeiterzahl", '(\d+)', 1)::int                          as emp_low,
                case when "Mitarbeiterzahl" ilike '%und mehr%' then 1000000
                     else regexp_extract("Mitarbeiterzahl", '-\s*(\d+)', 1)::int end        as emp_high,
                try_cast(replace("Stundensatz ArbMed",  ',', '.') as double)                as rate_arbmed,
                try_cast(replace("Stundensatz ArbSich", ',', '.') as double)                as rate_arbsich
            from padoa_prices
            where trim("Preismodel") = 'Variabel' and trim("Angebot") in ('ArbMed', 'ArbSich')
        ),
        padoa_pkg as (
            select trim("WZ Group") as bg,
                regexp_extract("Mitarbeiterzahl", '(\d+)', 1)::int                          as emp_low,
                case when "Mitarbeiterzahl" ilike '%und mehr%' then 1000000
                     else regexp_extract("Mitarbeiterzahl", '-\s*(\d+)', 1)::int end        as emp_high,
                try_cast(replace(regexp_replace("Wenn pauschal: ", '[^0-9,]', '', 'g'), ',', '.') as double) as pauschal_price
            from padoa_prices
            where trim("Angebot") = 'ArbMed und ArbSich' and trim("Preismodel") = 'Pauschal'
        ),
        fbc as (
            select mother_client_id, bg, child_client_id,
                   sum(replace(mitarbeiter, ',' ,'.')::float) over(partition by child_client_id) as total_mitarbeiter
            from (select distinct on(mother_client_id) mother_client_id, bg, mitarbeiter, child_client_id from full_basic_care)
        ),
        bc as (
            select fbc.bg as bg, fbc.total_mitarbeiter, bcp.id_easybill, bcp."Firmenname" as firmenname,
                case when bcp."Posten: Artikelnummer" like 'AM%' then 'ArbMed'
                     when bcp."Posten: Artikelnummer" like 'AS%' then 'ArbSich' end          as angebot,
                bcp.is_pauschal                                                              as pauschal,
                try_cast(replace(regexp_replace(bcp."Posten: Nettobetrag", '[^0-9,]', '', 'g'), ',', '.') as double) as netto,
                try_cast(replace(bcp."Posten: Anzahl", ',', '.') as double)                  as anzahl,
                try_cast(replace(bcp."hourly prices"::varchar, ',', '.') as double)          as hourly
            from all_clients_rebuilt bcp
            join fbc on fbc.mother_client_id = bcp.id_easybill
        ),
        -- pauschal: one row per (client, product) so AM-R / AS-R stay distinguishable,
        -- current_price = that product's netto. Padoa only prices the COMBINED package,
        -- so padoa_ref and adjustment_pct are computed at package level (combined AM+AS
        -- netto vs the package ref) and repeated on both product rows.
        pauschal_lines as (
            select bc.bg, any_value(bc.total_mitarbeiter) as employees, bc.id_easybill,
                   any_value(bc.firmenname) as firmenname, bc.angebot as product,
                   sum(bc.netto) as current_price
            from bc where bc.pauschal
            group by bc.id_easybill, bc.angebot, bc.bg
        ),
        pauschal_m as (
            select pl.bg, pl.employees, pl.id_easybill, pl.firmenname, 'pauschal' as model,
                   pl.product, pl.current_price, pkg.pauschal_price as padoa_ref,
                   round((pkg.pauschal_price
                          / nullif(sum(pl.current_price) over (partition by pl.id_easybill, pl.bg), 0)
                          - 1) * 100, 1) as adjustment_pct
            from pauschal_lines pl
            left join padoa_pkg pkg
                   on pkg.bg = pl.bg and pl.employees >= pkg.emp_low and pl.employees <= pkg.emp_high
        ),
        -- variabel: one row per (client, product); effective hourly = sum(netto) / sum(anzahl)
        -- across all AM-R / AS-R lines of that product on the invoice.
        variabel as (
            select *, round((padoa_ref / nullif(current_price, 0) - 1) * 100, 1) as adjustment_pct
            from (
                select bc.bg, any_value(bc.total_mitarbeiter) as employees, bc.id_easybill,
                       any_value(bc.firmenname) as firmenname, 'variabel' as model, bc.angebot as product,
                       round(sum(bc.netto) / nullif(sum(bc.anzahl), 0)) as current_price,
                       case when bc.angebot = 'ArbMed' then coalesce(any_value(pv.rate_arbmed), 120)
                            else coalesce(any_value(pv.rate_arbsich), 80) end as padoa_ref
                from bc
                left join padoa_var pv
                       on pv.bg = bc.bg and pv.angebot = bc.angebot
                      and bc.total_mitarbeiter >= pv.emp_low and bc.total_mitarbeiter <= pv.emp_high
                where not bc.pauschal
                group by bc.id_easybill, bc.angebot, bc.bg
            )
        )
        select bg, employees, id_easybill, firmenname, model, product, current_price, padoa_ref, adjustment_pct
        from (select * from pauschal_m union all select * from variabel)
        order by id_easybill, product
        """
    )
    duck.sql("select * from discounts").to_csv(str(DISCOUNTS_OUT))
    n_rows = duck.sql("select count(*) from discounts").fetchone()[0]
    print(f"✓ basic_care_discounts.csv — {n_rows} rows")


if __name__ == "__main__":
    main()
