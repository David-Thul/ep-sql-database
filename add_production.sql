-- 1. PRODUCTION (Partitioned)
CREATE TABLE production_monthly (
    wellbore_id         UUID NOT NULL,
    prod_date           DATE NOT NULL,
    oil_vol             REAL, -- 'UNIT: Barrels (bbl).'
    gas_vol             REAL, -- 'UNIT: Thousand Cubic Feet (Mcf).'
    water_vol           REAL, -- 'UNIT: Barrels (bbl).'
    days_on             SMALLINT,
    
    PRIMARY KEY (wellbore_id, prod_date)
) PARTITION BY HASH (wellbore_id);

-- Create Partitions (Example: 4 partitions)
CREATE TABLE production_p0 PARTITION OF production_monthly FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE production_p1 PARTITION OF production_monthly FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE production_p2 PARTITION OF production_monthly FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE production_p3 PARTITION OF production_monthly FOR VALUES WITH (MODULUS 4, REMAINDER 3);