-- 02_build_production.sql
\set prod_start_date '''2020-01-01'''

-- A. MONTHLY PRODUCTION
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

-- B. DAILY PRODUCTION
CREATE TABLE production_daily (
    wellbore_id         UUID NOT NULL REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    prod_date           DATE NOT NULL,
    
    -- Volumes
    oil_vol             REAL DEFAULT 0,
    gas_vol             REAL DEFAULT 0,
    water_vol           REAL DEFAULT 0,
    
    -- Engineering
    hours_on            REAL CHECK (hours_on >= 0 AND hours_on <= 24),
    tubing_pressure     REAL,
    casing_pressure     REAL,
    choke_size          REAL,
    downtime_code       VARCHAR(50),
    comments            TEXT,
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY RANGE (prod_date);

-- C. PARTITION MAKER (Run Once)
DO $$
DECLARE
    v_start_date DATE := :prod_start_date; 
    v_start_year INT := EXTRACT(YEAR FROM v_start_date);
    v_curr_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
    v_iter INT;
BEGIN
    -- Legacy Partitions
    EXECUTE format('CREATE TABLE production_monthly_legacy PARTITION OF production_monthly FOR VALUES FROM (MINVALUE) TO (%L)', v_start_date);
    EXECUTE format('CREATE TABLE production_daily_legacy PARTITION OF production_daily FOR VALUES FROM (MINVALUE) TO (%L)', v_start_date);

    -- Yearly Partitions
    v_iter := v_start_year;
    WHILE v_iter <= (v_curr_year + 2) LOOP
        EXECUTE format('CREATE TABLE production_monthly_y%s PARTITION OF production_monthly FOR VALUES FROM (%L) TO (%L)', v_iter, make_date(v_iter,1,1), make_date(v_iter+1,1,1));
        EXECUTE format('CREATE TABLE production_daily_y%s PARTITION OF production_daily FOR VALUES FROM (%L) TO (%L)', v_iter, make_date(v_iter,1,1), make_date(v_iter+1,1,1));
        v_iter := v_iter + 1;
    END LOOP;
    
    -- Default Partitions
    EXECUTE 'CREATE TABLE production_monthly_default PARTITION OF production_monthly DEFAULT';
    EXECUTE 'CREATE TABLE production_daily_default PARTITION OF production_daily DEFAULT';
END $$;