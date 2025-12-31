-- ==========================================
-- 1. CONFIGURATION (CHANGE THIS DATE)
-- ==========================================
-- This sets the "Modern Era" start date.
-- All data before this date goes into the 'production_legacy' table.
-- All data after this date gets its own yearly table.
\set modern_start_date '''2020-01-01'''

-- ==========================================
-- 2. PARENT TABLE
-- ==========================================
DROP TABLE IF EXISTS production_monthly CASCADE;

CREATE TABLE production_monthly (
    wellbore_id         UUID NOT NULL,
    prod_date           DATE NOT NULL,
    
    oil_vol             REAL DEFAULT 0, -- 'UNIT: Barrels (bbl).'
    gas_vol             REAL DEFAULT 0, -- 'UNIT: Thousand Cubic Feet (Mcf).'
    water_vol           REAL DEFAULT 0, -- 'UNIT: Barrels (bbl).'
    
    days_on             SMALLINT,
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Partition Key MUST be in PK
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY RANGE (prod_date);

-- ==========================================
-- 3. AUTOMATED PARTITION CREATION
-- ==========================================
DO $$
DECLARE
    -- Read the psql variable (passed as string literal)
    v_start_date DATE := :modern_start_date; 
    v_current_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
    v_start_year INT := EXTRACT(YEAR FROM v_start_date);
    v_year_iterator INT;
    v_sql TEXT;
BEGIN
    -- A. CREATE LEGACY PARTITION
    -- Holds everything from the beginning of time until the start date.
    v_sql := format('CREATE TABLE production_legacy PARTITION OF production_monthly 
                    FOR VALUES FROM (MINVALUE) TO (%L)', v_start_date);
    EXECUTE v_sql;
    RAISE NOTICE 'Created production_legacy (MINVALUE to %)', v_start_date;

    -- B. CREATE YEARLY PARTITIONS (From Start Year to Current Year + 2)
    -- We loop from the start year up to 2 years in the future to be safe.
    v_year_iterator := v_start_year;
    
    WHILE v_year_iterator <= (v_current_year + 2) LOOP
        v_sql := format('CREATE TABLE production_y%s PARTITION OF production_monthly 
                        FOR VALUES FROM (%L) TO (%L)', 
                        v_year_iterator, 
                        make_date(v_year_iterator, 1, 1), 
                        make_date(v_year_iterator + 1, 1, 1));
        EXECUTE v_sql;
        RAISE NOTICE 'Created production_y% (Year %)', v_year_iterator, v_year_iterator;
        
        v_year_iterator := v_year_iterator + 1;
    END LOOP;

    -- C. CREATE DEFAULT PARTITION
    -- Safety net for dates far in the future or nulls (though date is NOT NULL)
    EXECUTE 'CREATE TABLE production_default PARTITION OF production_monthly DEFAULT';
    RAISE NOTICE 'Created production_default';

END $$;

-- ==========================================
-- 4. INDEXING
-- ==========================================
-- BRIN index is perfect for time-series data physically ordered by time
-- (which production data usually is when loaded).
CREATE INDEX idx_prod_date_brin ON production_monthly USING BRIN(prod_date);

-- Comment to explain the structure to future users
COMMENT ON TABLE production_monthly IS 'Partitioned by Range. Change "modern_start_date" in SQL file to adjust legacy cutoff.';