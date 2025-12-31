-- ==========================================
-- 1. COMPLETIONS & PERFORATIONS
-- ==========================================
CREATE TABLE completions (
    completion_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    completion_date     DATE,
    completion_name     VARCHAR(100), -- 'Stage 1', 'Upper Zone'
    
    -- Intervals
    top_md              REAL NOT NULL, -- 'UNIT: Feet (Imperial).'
    base_md             REAL NOT NULL, -- 'UNIT: Feet (Imperial).'
    
    -- Details
    perf_count          INTEGER, -- Number of holes
    perf_diameter       REAL,
    stimulation_type    VARCHAR(50), -- 'Hydraulic Frac', 'Acid', 'None'
    
    -- Frac Data (Proppant, Fluid Vol)
    proppant_lbs        REAL,
    fluid_gal           REAL,
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for finding "What wells are open in the Eagle Ford?"
-- (Requires joining to Tops, but the index on wellbore helps)
CREATE INDEX idx_completions_wb ON completions(wellbore_id);