-- ==========================================
-- ADD DAILY PRODUCTION SCHEMA
-- ==========================================

-- 1. CONFIGURATION
-- This sets the start date for the active partition loop.
-- Data prior to this goes into 'production_daily_legacy'.
\set daily_start_date '2020-01-01'

-- 2. PARENT TABLE
DROP TABLE IF EXISTS production_daily CASCADE;

CREATE TABLE production_daily (
    wellbore_id         UUID NOT NULL REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    prod_date           DATE NOT NULL,
    
    -- Volumes
    oil_vol             REAL DEFAULT 0, -- 'UNIT: Barrels (bbl).'
    gas_vol             REAL DEFAULT 0, -- 'UNIT: Thousand Cubic Feet (Mcf).'
    water_vol           REAL DEFAULT 0, -- 'UNIT: Barrels (bbl).'
    
    -- Operational Metrics (The Engineering Diagnostics)
    hours_on            REAL CHECK (hours_on >= 0 AND hours_on <= 24),
    tubing_pressure     REAL, -- 'UNIT: PSI.'
    casing_pressure     REAL, -- 'UNIT: PSI.'
    choke_size          REAL, -- 'UNIT: 64ths of an inch.'
    
    -- Downtime Context
    downtime_code       VARCHAR(50), -- e.g., 'Weather', 'Pump Failure'
    comments            TEXT,
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Partition Key (Must be part of PK)
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY RANGE (prod_date);

-- 3. AUTOMATED PARTITION CREATION
DO $$
DECLARE
    v_start_date DATE := :daily_start_date; 
    v_current_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
    v_start_year INT := EXTRACT(YEAR FROM v_start_date);
    v_year_iterator INT;
    v_sql TEXT;
BEGIN
    -- A. CREATE LEGACY PARTITION
    -- Holds historical daily data (often sparse or reconstructed).
    v_sql := format('CREATE TABLE production_daily_legacy PARTITION OF production_daily 
                    FOR VALUES FROM (MINVALUE) TO (%L)', v_start_date);
    EXECUTE v_sql;
    RAISE NOTICE 'Created production_daily_legacy (MINVALUE to %)', v_start_date;

    -- B. CREATE YEARLY PARTITIONS
    -- Generates tables for Start Year -> Current Year + 1 (Future buffer)
    v_year_iterator := v_start_year;
    
    WHILE v_year_iterator <= (v_current_year + 1) LOOP
        v_sql := format('CREATE TABLE production_daily_y%s PARTITION OF production_daily 
                        FOR VALUES FROM (%L) TO (%L)', 
                        v_year_iterator, 
                        make_date(v_year_iterator, 1, 1), 
                        make_date(v_year_iterator + 1, 1, 1));
        EXECUTE v_sql;
        RAISE NOTICE 'Created production_daily_y% (Year %)', v_year_iterator, v_year_iterator;
        
        v_year_iterator := v_year_iterator + 1;
    END LOOP;

    -- C. CREATE DEFAULT PARTITION
    -- Catch-all for accidental future dates beyond our loop
    EXECUTE 'CREATE TABLE production_daily_default PARTITION OF production_daily DEFAULT';
    RAISE NOTICE 'Created production_daily_default';

END $$;

-- 4. INDEXING
-- BRIN index is highly efficient for time-series where data is loaded chronologically.
CREATE INDEX idx_prod_daily_date_brin ON production_daily USING BRIN(prod_date);

-- Standard B-Tree for fast single-well lookups (e.g. "Show me Well X's history")
CREATE INDEX idx_prod_daily_wb ON production_daily(wellbore_id);