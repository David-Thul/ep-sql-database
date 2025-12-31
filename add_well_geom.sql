-- ==========================================
-- 1. DIRECTIONAL SURVEYS (Raw Data)
-- ==========================================
-- This holds the raw measurements used to calculate the 3D Geometry
CREATE TABLE directional_surveys (
    survey_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    -- The Header
    survey_company      VARCHAR(100),
    survey_date         DATE,
    survey_type         VARCHAR(50), -- 'MWD', 'Gyro', 'Totco'
    is_active           BOOLEAN DEFAULT TRUE, -- Which survey do we trust?
    
    -- The Array Approach (Minimalist & Fast)
    -- Storing 1000 survey points as JSONB or Arrays is cleaner than 
    -- 1000 rows per well for simple storage.
    -- OSDU Compatible format: List of objects
    -- [{"md": 0, "inc": 0, "azi": 0}, {"md": 100, ...}]
    survey_points       JSONB NOT NULL, 
    
    -- Quality Checks
    azimuth_ref         VARCHAR(20) DEFAULT 'True North',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

