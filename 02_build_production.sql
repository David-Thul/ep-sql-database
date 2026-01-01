-- ======================================================================
-- SCRIPT: 02_build_production.sql
-- PURPOSE: Time-series production data.
-- DEPENDENCIES: 01_build_core.sql
-- ======================================================================

-- 1. MONTHLY PRODUCTION (Partitioned)
CREATE TABLE production_monthly (
    wellbore_id         UUID NOT NULL REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    prod_date           DATE NOT NULL,
    oil_vol             REAL DEFAULT 0,
    gas_vol             REAL DEFAULT 0,
    water_vol           REAL DEFAULT 0,
    days_on             SMALLINT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY RANGE (prod_date);

-- 2. DAILY PRODUCTION (Partitioned)
CREATE TABLE production_daily (
    wellbore_id         UUID NOT NULL REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    prod_date           DATE NOT NULL,
    
    oil_vol             REAL, -- Null allowed (distinguish 0 from missing)
    gas_vol             REAL,
    water_vol           REAL,
    
    hours_on            REAL CHECK (hours_on >= 0 AND hours_on <= 24),
    flow_status         VARCHAR(50), -- 'Flowing', 'Pumping', 'Shut-In'
    
    tubing_pressure     REAL,
    casing_pressure     REAL,
    choke_size          REAL,
    downtime_code       VARCHAR(50),
    
    comments            TEXT,
    attributes          JSONB DEFAULT '{}',
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY RANGE (prod_date);

-- 3. INDEXES
CREATE INDEX idx_prod_daily_wb ON production_daily(wellbore_id);
CREATE INDEX idx_prod_monthly_wb ON production_monthly(wellbore_id);

-- 4. PARTITION INITIALIZATION
-- Automatically creates partitions for years 2010 through Current Year + 2
DO $$
DECLARE
    v_start_year INT := 2010;
    v_curr_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
    v_iter INT;
BEGIN
    -- Create Default Partitions (Catch-all for weird dates)
    EXECUTE 'CREATE TABLE IF NOT EXISTS production_daily_default PARTITION OF production_daily DEFAULT';
    EXECUTE 'CREATE TABLE IF NOT EXISTS production_monthly_default PARTITION OF production_monthly DEFAULT';

    -- Create Yearly Partitions
    v_iter := v_start_year;
    WHILE v_iter <= (v_curr_year + 2) LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS production_monthly_y%s PARTITION OF production_monthly FOR VALUES FROM (%L) TO (%L)', v_iter, make_date(v_iter,1,1), make_date(v_iter+1,1,1));
        EXECUTE format('CREATE TABLE IF NOT EXISTS production_daily_y%s PARTITION OF production_daily FOR VALUES FROM (%L) TO (%L)', v_iter, make_date(v_iter,1,1), make_date(v_iter+1,1,1));
        v_iter := v_iter + 1;
    END LOOP;
END $$;