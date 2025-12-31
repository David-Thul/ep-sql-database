-- ==========================================
-- 1. ROCK SAMPLES (Enhanced Inventory)
-- ==========================================
-- This table tracks the physical object. 
-- "Parent" for all rock analysis.
CREATE TABLE rock_samples (
   sample_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    sample_type         VARCHAR(50), -- 'Core', 'Cuttings'
    sample_name         VARCHAR(100),
    
    top_depth_md        REAL, --'UNIT: Feet (Imperial).'
    base_depth_md       REAL, --'UNIT: Feet (Imperial).'
    
    storage_location    VARCHAR(255),
    attributes          JSONB DEFAULT '{}',
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 2. ROUTINE CORE ANALYSIS (CCA)
-- ==========================================
-- Standard columns for the most common queries.
CREATE TABLE analysis_routine_core (
    analysis_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sample_id           UUID REFERENCES rock_samples(sample_id) ON DELETE CASCADE,
    
    porosity            REAL, --'UNIT: Percentage (0-100).'
    permeability_kair   REAL, --'UNIT: mD.'
    grain_density       REAL, --'UNIT: g/cc.'
    sat_oil             REAL, --'UNIT: Percentage (0-100).'
    sat_water           REAL, --'UNIT: Percentage (0-100).'
    
    lab_name            VARCHAR(100),
    analysis_date       DATE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- 3. SOURCE ROCK GEOCHEM (Pyrolysis/TOC)
-- ==========================================
-- Essential for unconventional plays.
CREATE TABLE analysis_source_rock (
    analysis_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sample_id           UUID REFERENCES rock_samples(sample_id) ON DELETE CASCADE,
    
    toc                 REAL, -- Total Organic Carbon (%)
    
    -- Pyrolysis Data
    s1                  REAL, -- Free Hydrocarbons
    s2                  REAL, -- Kerogen Potential
    s3                  REAL, -- CO2 yield
    tmax                REAL, -- Temperature at max S2 (°C)
    
    -- Computed Indices (Computed on load or via View)
    hi                  REAL, -- Hydrogen Index (S2/TOC * 100)
    oi                  REAL, -- Oxygen Index (S3/TOC * 100)
    pi                  REAL, -- Production Index (S1 / (S1+S2))
    
    vitrinite_reflectance REAL, -- Ro (%)
    kerogen_type        VARCHAR(10) -- Type I, II, III
);

-- ==========================================
-- 4. MINERALOGY (XRD) & SPECIAL CORE (SCAL)
-- ==========================================
-- Using JSONB allows flexible storage of mineral lists without 
-- creating 50 columns for every possible mineral.
CREATE TABLE analysis_special_core (
    analysis_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sample_id           UUID REFERENCES rock_samples(sample_id) ON DELETE CASCADE,
    
    analysis_type       VARCHAR(50), -- 'XRD', 'Capillary Pressure', 'Wettability'
    
    -- Flexible Data Store
    -- XRD Example: {"quartz": 45.2, "calcite": 12.0, "clay_total": 30.5}
    -- SCAL Example: {"pc_curve": [0.1, 0.5, 1.0], "sw_curve": [1.0, 0.8, 0.2]}
    measurement_data    JSONB NOT NULL,
    
    comments            TEXT
);

-- ==========================================
-- 5. FLUID SAMPLES & GEOCHEM
-- ==========================================
CREATE TABLE fluid_samples (
    fluid_sample_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id),
    
    sample_type         VARCHAR(50), -- 'Oil', 'Gas', 'Condensate', 'Produced Water'
    sample_date         DATE,
    depth_md            REAL,
    test_type           VARCHAR(50), -- 'DST', 'RFT', 'Production Header'
    
    -- Basic Properties
    api_gravity         REAL,
    sulfur_pct          REAL,
    salinity_tds        REAL
);

CREATE TABLE analysis_fluid_detail (
    analysis_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fluid_sample_id     UUID REFERENCES fluid_samples(fluid_sample_id),
    
    analysis_category   VARCHAR(50), -- 'Isotopes', 'SARA', 'Chromatography'
    
    -- Example: {"C1": 70.2, "C2": 12.1, "delta_C13": -32.5}
    results_data        JSONB
);

-- ==========================================
-- 6. PETROPHYSICAL ZONE PARAMETERS
-- ==========================================
-- This links a Formation (Zone) in a specific Well to interpretation parameters.
-- It answers: "What Rw did we use for the Eagle Ford in Well A?"
CREATE TABLE petrophysics_zone_params (
    param_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    strat_unit_id       INTEGER REFERENCES strat_unit_dictionary(strat_unit_id),
    
    model_name          VARCHAR(100),
    is_official         BOOLEAN DEFAULT FALSE,
    interpreter         VARCHAR(100),
    
    -- Parameters
    rw                  REAL, 
    rw_temp             REAL, -- 'UNIT: Degrees Fahrenheit.'
    m                   REAL, 
    n                   REAL, 
    a                   REAL, 
    
    -- Averages
    avg_porosity        REAL, -- 'UNIT: Percentage (0-100).'
    avg_sw              REAL, -- 'UNIT: Percentage (0-100).'
    net_pay             REAL, -- 'UNIT: Feet (Imperial).'
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_well_zone_model UNIQUE (wellbore_id, strat_unit_id, model_name)
);
-- Partial Index for Official Flag
CREATE UNIQUE INDEX uq_official_petrophysics 
ON petrophysics_zone_params (wellbore_id, strat_unit_id) 
WHERE is_official = TRUE;

-- ==========================================
-- 7. Core & Cuttings Descriptions (Interval Data)
-- ==========================================
--This allows you to digitize those hand-drawn core description sheets.
CREATE TABLE core_descriptions (
    desc_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    -- Interval
    top_depth_md        REAL,
    base_depth_md       REAL,
    
    -- Primary Lithology (Standardized for coloring logs)
    primary_lithology   VARCHAR(50), -- 'Sandstone', 'Limestone', 'Shale'
    
    -- The Detail
    description         TEXT, -- "Light gray, fine grained, sub-rounded..."
    
    -- Sedimentary Structures (Array allows multiple)
    structures          TEXT[], -- ['Cross-bedding', 'Bioturbation', 'Fractures']
    
    -- Oil Show
    show_type           VARCHAR(50), -- 'Fluorescence', 'Cut', 'Stain'
    show_quality        VARCHAR(20)  -- 'Good', 'Fair', 'Trace', 'None'
);

-- Index for fast retrieval of descriptions by depth
CREATE INDEX idx_core_desc_depth ON core_descriptions (wellbore_id, top_depth_md);

-- ==========================================
-- 8. Thin Section & Petrography
-- ==========================================
-- Thin sections are distinct because they are usually tied to a specific rock_sample (the plug).
CREATE TABLE analysis_thin_section (
    analysis_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sample_id           UUID REFERENCES rock_samples(sample_id) ON DELETE CASCADE,
    
    -- Metadata
    analyst             VARCHAR(100),
    
    -- Textural Data
    grain_size_mean     VARCHAR(50), -- 'Fine', 'Medium', 'Coarse' or numerical microns
    sorting             VARCHAR(50), -- 'Well', 'Poor'
    rounding            VARCHAR(50), -- 'Sub-angular', 'Rounded'
    
    -- Composition (Point Count Results)
    -- JSONB is perfect here because constituents vary by basin.
    -- Example: {"quartz": 65, "feldspar": 10, "lithics": 5, "calcite_cement": 10, "porosity": 10}
    composition_data    JSONB DEFAULT '{}',
    
    -- Dunham/Folk Classification (for Carbonates)
    carbonate_class     VARCHAR(50), -- 'Grainstone', 'Packstone'
    
    comments            TEXT
);

-- ==========================================
-- 9. Biostratigraphy (Paleo)
-- ==========================================
-- Enable the use of "Biozones" rather than just formation names.
CREATE TABLE analysis_biostrat (
    paleo_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id),
    
    depth_md            REAL,
    
    fossil_name         VARCHAR(100), -- 'Globotruncana', 'Nannofossils'
    biozone             VARCHAR(100),
    abundance           VARCHAR(50),  -- 'Rare', 'Common', 'Abundant'
    age_epoch           VARCHAR(50)   -- 'Late Cretaceous', 'Campanian'
);

-- ==========================================
-- 10. Media Catelog (Photos, Reports, Etc)
-- ==========================================
-- Do not put JPGs in Postgres. Use this catalog to link to your file server or S3 bucket.
CREATE TABLE media_catalog (
    media_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id),
    
    media_type          VARCHAR(50), -- 'Photo', 'Report', 'Core Plug Scan'
    file_format         VARCHAR(20), -- 'JPG', 'PDF', 'TIFF'
    file_path           TEXT NOT NULL, -- S3 or File Server Path
    
    top_depth_md        REAL, -- 'UNIT: Feet (Imperial).'
    base_depth_md       REAL, -- 'UNIT: Feet (Imperial).'
    description         TEXT, -- 'Core photo of sample 1234...'
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);