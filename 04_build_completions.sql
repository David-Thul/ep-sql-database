-- ======================================================================
-- SCRIPT: 04_build_completions.sql
-- PURPOSE: Frac, Perfs, and Completion strings.
-- DEPENDENCIES: 01_build_core.sql
-- ======================================================================

CREATE TABLE completions (
    completion_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    completion_date     DATE,
    completion_name     VARCHAR(100), -- 'Stage 1', 'Upper Zone'
    
    -- Intervals
    top_md              REAL NOT NULL,
    base_md             REAL NOT NULL,
    
    -- Stimulation Details
    stimulation_type    VARCHAR(50), -- 'Hydraulic Frac', 'Acid', 'None'
    proppant_lbs        REAL,
    fluid_gal           REAL,
    
    -- Flex fields for detailed chemical composition or perf gun specs
    attributes          JSONB DEFAULT '{}',
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_completions_wb ON completions(wellbore_id);