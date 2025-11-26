-- 1️⃣ Crée (ou remplace) la table de sortie
DROP TABLE IF EXISTS public.table_column_stats;

CREATE TABLE public.table_column_stats (
    table_name text,
    column_name text,
    total_rows bigint,
    distinct_values bigint,
	list_distinct_values text[],
    calculated_at timestamp default now()
);

-- 2️⃣ Script PL/pgSQL pour remplir cette table
DO $$
DECLARE
    r RECORD;  -- table
    c RECORD;  -- colonne
    total_rows BIGINT;
    distinct_count BIGINT;
	list_distinct_values TEXT[];
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'  -- 🔧 change ici si besoin
          AND table_type = 'BASE TABLE'
    LOOP
        -- nombre total de lignes
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', r.table_schema, r.table_name)
        INTO total_rows;

        -- si table vide, on enregistre juste le total
        IF total_rows = 0 THEN
        	EXECUTE format('DROP TABLE %I.%I', r.table_schema, r.table_name);
        ELSE
            -- sinon, pour chaque colonne, on compte les valeurs distinctes
            FOR c IN
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = r.table_schema
                  AND table_name = r.table_name
            LOOP
                EXECUTE format('SELECT COUNT(DISTINCT %I) FROM %I.%I', c.column_name, r.table_schema, r.table_name)
                INTO distinct_count;
				IF distinct_count < 30 THEN
					EXECUTE format('SELECT ARRAY_AGG(DISTINCT %I::text) FROM %I.%I', c.column_name, r.table_schema, r.table_name)
					INTO list_distinct_values;
					INSERT INTO public.table_column_stats(table_name, column_name, total_rows, distinct_values, list_distinct_values)
                	VALUES ( r.table_name, c.column_name, total_rows, distinct_count, list_distinct_values);
				ELSE
					INSERT INTO public.table_column_stats( table_name, column_name, total_rows, distinct_values)
                	VALUES ( r.table_name, c.column_name, total_rows, distinct_count);
	
                END IF;
            END LOOP;
        END IF;
    END LOOP;
END $$;