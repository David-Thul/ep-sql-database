-- ======================================================================
-- SCRIPT: 01_build_core.sql
-- PURPOSE: Foundational objects. Must be run first.
-- DEPENDENCIES: None
-- ======================================================================

-- 1. EXTENSIONS
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. WELL MASTER (Surface Location)
-- The physical "head" of the well.
CREATE TABLE well_master (
    well_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    uwi                 VARCHAR(50) UNIQUE NOT NULL, 
    well_name           VARCHAR(255),
    operator            VARCHAR(255),
    spud_date           DATE,
    
    -- GIS: Use Geometry for spatial ops, but store raw Lat/Lon as Double for precision
    surface_geom        GEOMETRY(POINT, 4269), -- NAD83
    lat_surface         DOUBLE PRECISION,
    lon_surface         DOUBLE PRECISION,
    
    elevation_kb        REAL, 
    elevation_gl        REAL,
    
    attributes          JSONB DEFAULT '{}', -- The "Catch-All"
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. WELLBORE MASTER (The 3D Hole)
-- A well can have multiple wellbores (sidetracks, laterals).
CREATE TABLE wellbore_master (
    wellbore_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    well_id             UUID REFERENCES well_master(well_id) ON DELETE CASCADE,
    wellbore_name       VARCHAR(50) DEFAULT 'OH', 
    
    -- Depths
    total_depth_md      REAL,
    total_depth_tvd     REAL,
    
    -- Trajectory Geometry
    -- Z-Axis: Recommend storing as Subsea True Vertical Depth (Negative downwards)
    trajectory_geom     GEOMETRY(LINESTRINGZ, 4269),
    
    -- Spatial Metadata (Critical for computing the trajectory)
    crs_epsg            INTEGER,      
    grid_convergence    REAL,         
    
    attributes          JSONB DEFAULT '{}',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_well_bore UNIQUE(well_id, wellbore_name)
);

-- 4. DIRECTIONAL SURVEYS
-- The raw data used to calculate the trajectory in wellbore_master.
CREATE TABLE directional_surveys (
    survey_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wellbore_id         UUID REFERENCES wellbore_master(wellbore_id) ON DELETE CASCADE,
    
    survey_company      VARCHAR(100),
    survey_date         DATE,
    survey_type         VARCHAR(50), -- 'MWD', 'Gyro'
    is_active           BOOLEAN DEFAULT TRUE,
    
    -- Orientation Data
    azimuth_ref         VARCHAR(20) DEFAULT 'Grid North', 
    north_ref_offset    REAL DEFAULT 0.0,
    
    -- Dense Data Storage
    -- Array of Objects: [{"md": 0, "inc": 0, "azi": 0}, ...]
    survey_points       JSONB NOT NULL, 
    
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. INDEXES
CREATE INDEX idx_well_uwi ON well_master(uwi);
CREATE INDEX idx_well_geom ON well_master USING GIST (surface_geom);
CREATE INDEX idx_wb_well ON wellbore_master(well_id);