from merge_tables.db.connection import connect_to_postgres_via_duckdb
from merge_tables.db.tables import create_clean_account_name_macro
from duckdb.sqltypes import VARCHAR
import duckdb
from pathlib import Path


MERGE_TABLES_DIR = Path(__file__).parent.parent
DATA_DIR = MERGE_TABLES_DIR / "data"
OUTPUT_DIR = MERGE_TABLES_DIR / "output"

OUTPUT_DIR.mkdir(exist_ok=True)

CITIES = [
    "berlin", 
    "dusseldorf",
    "frankfurt",
    "hamburg",
    "kiel",
    "koln",
    "munchen",
    "rostock",
    "stuttgart",
    "viersen"
    ]


def init_easybill_tables(duck: duckdb.DuckDBPyConnection) -> None:
    """Initialize easybill tables from CSV files."""
    easybill_contacts_file = DATA_DIR / "easybill_active_contracts.csv"
    easybill_all_docs_file = DATA_DIR / "easybill_all_archived_docs.csv"
    
    if not easybill_contacts_file.exists():
        raise FileNotFoundError(f"File not found: {easybill_contacts_file}")
    if not easybill_all_docs_file.exists():
        raise FileNotFoundError(f"File not found: {easybill_all_docs_file}")
    
    # Use absolute paths for DuckDB
    duck.execute(f"""
        create table if not exists easybill_contacts as
        select 
            distinct on("Kontakt: Kundennummer") 
            "Kontakt: Kundennummer", 
            "Kontakt: Firma", 
            "Kontakt: Straße/Hausnummer", 
            "Kontakt: Postleitzahl", 
            "Kontakt: Ort", 
            "Kontakt: E-Mail" 
        from read_csv('{easybill_contacts_file.as_posix()}')
    """)
    duck.execute(f"""
        create table if not exists easybill_all_docs as
        select 
            replace("Kontakt: Kundennummer", ' ', '') as id_client_easybill,
            "Kontakt: Firma" as mother_entity_easybill,
            "Posten: Artikelnummer" as id_article_easybill,
            "Posten: Artikelbeschreibung" as article_description_easybill,
        from read_csv('{easybill_all_docs_file.as_posix()}', types={{'Kontakt: Kundennummer': 'VARCHAR'}})
    """)


def match_medisoft_child_firms_with_easybill_mother_firms(
        duck: duckdb.DuckDBPyConnection, 
        city_name: str, 
        input_file_path: str | Path | None = None, 
        output_file_path: str | Path | None = None
    ) -> None:
    """Match medisoft child firms with easybill mother firms for a given city."""
    print(f"Matching medisoft child firms with easybill mother firms for {city_name}")

    # Set default paths if not provided
    if input_file_path is None:
        input_file_path = DATA_DIR / f"{city_name}_active_contracts.csv"
    else:
        # Convert string to Path if needed, and resolve relative paths
        input_file_path = Path(input_file_path)
        if not input_file_path.is_absolute():
            input_file_path = DATA_DIR / input_file_path
    
    if output_file_path is None:
        output_file_path = OUTPUT_DIR / f"{city_name}_firms_consolidated.csv"
    else:
        # Convert string to Path if needed, and resolve relative paths
        output_file_path = Path(output_file_path)
        if not output_file_path.is_absolute():
            output_file_path = OUTPUT_DIR / output_file_path

    # Ensure output directory exists
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file '{input_file_path}' does not exist.")
    
    # Use absolute path for DuckDB
    input_file_path_str = input_file_path.as_posix()
    
    duck.sql(
        f"""
        with easybill_{city_name} as (
            select
                left({city_name}_active_contracts."Kd-Nr."::varchar, 9) as kd_nr,
                * exclude(column00, column01, column02) 
            from read_csv('{input_file_path_str}', skip=4, strict_mode=false) as {city_name}_active_contracts
            join easybill_contacts as ec 
                on left({city_name}_active_contracts."Kd-Nr."::varchar, 9) = ec."Kontakt: Kundennummer"
            where Unternehmen is not null
        ), clean_med_firms as (
            select 
                rec_id, name,
                clean_account_name(coalesce(split(pfad, '/')[2], split(pfad, '/')[1], name)) as clean_name,
                kuerzel, pfad, strasse, plz
            from pg.medisoft.table_firmenstruktur
            where pfad like 'BSH {city_name.capitalize()}%'
        ), mother_entities as (
            select 
                kd_nr,
                Unternehmen,
                clean_account_name(Unternehmen) as entity_name
            from easybill_{city_name}
        ), matched_firms as (
            select 
                me.kd_nr, me.Unternehmen as mother_entity, me.entity_name, m.rec_id, 
                m.name, m.kuerzel, m.clean_name, m.pfad, 
                jaro_winkler_similarity(m.clean_name, me.entity_name) as sim
            from mother_entities as me
            left join clean_med_firms as m
                on m.clean_name = me.entity_name 
                or jaro_winkler_similarity(m.clean_name, me.entity_name) > 0.9
                or m.clean_name ilike '%'||me.entity_name||'%'
            where sim > 0.6
            qualify row_number() over (partition by m.rec_id order by sim desc) = 1
            order by kd_nr
        ), {city_name}_firms_found as (
            select 
                m.kd_nr as id_easybill,
                m.Unternehmen as mother_entity_easybill,
                m.entity_name as clean_entity_name_easybill,
                me.rec_id as id_medisoft,
                coalesce(me.name, me.kuerzel) as name_medisoft,
                coalesce(split(me.pfad, '/')[2], split(me.pfad, '/')[1], me.name) as mother_entity_medisoft,
                me.clean_name as clean_entity_name_medisoft,
                me.pfad as pfad_medisoft
            from mother_entities m
            join matched_firms as me
                on me.kd_nr = m.kd_nr
        ), {city_name}_firms_consolidated as (
            select
                eb.kd_nr as id_easybill,
                id_medisoft,
                Unternehmen as eb_mother_firm,
                name_medisoft,
                mother_entity_medisoft,
                pfad_medisoft,
                concat_ws(' ', "Kontakt: Straße/Hausnummer", "Kontakt: Postleitzahl", "Kontakt: Ort") as eb_address,
                concat_ws(' ', strasse, plz, ort) as medisoft_operating_firm_address,
            from easybill_{city_name} as eb 
            left join {city_name}_firms_found 
                on {city_name}_firms_found.id_easybill = eb.kd_nr
            left join pg.medisoft.table_firmenstruktur as mf
                on mf.rec_id = {city_name}_firms_found.id_medisoft
        )
        select 
            ec."Kontakt: Kundennummer" as id_easybill, 
            bf.id_medisoft,
            case when sum(
                case 
                    when ead.id_article_easybill ilike '%-OMW%' then 1
                    else 0
                end
            ) > 0 then true else false end as should_have_medisoft_firm,
            ec."Kontakt: Firma" as eb_mother_firm,
            bf.name_medisoft,
            bf.eb_address,
            bf.medisoft_operating_firm_address,
        from easybill_contacts as ec
        left join easybill_all_docs as ead
            on ec."Kontakt: Kundennummer" = ead.id_client_easybill
        join {city_name}_firms_consolidated as bf
            on ec."Kontakt: Kundennummer" = bf.id_easybill
        group by all
        order by 4
        """
    ).to_csv(str(output_file_path))
    
    print(f"✓ Output saved to: {output_file_path}")


if __name__ == "__main__":
    duck = connect_to_postgres_via_duckdb()
    create_clean_account_name_macro(duck)
    init_easybill_tables(duck)
    for city_name in CITIES:
        match_medisoft_child_firms_with_easybill_mother_firms(duck, city_name)
