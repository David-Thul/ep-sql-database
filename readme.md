# Open Subsurface Lite Database (OSLDB)
### The OSDU Quagmire Antidote

A modular, schema-agnostic data "Outhouse" for subsurface energy data. This platform combines the relational structure of PostgreSQL/PostGIS for metadata and spatial queries with the speed of Parquet for high-density curve data.

---

## Critical Warnings and Configuration Notes

**Please review the following configuration assumptions before ingesting data.**

### 1. Strict Unit Assumption: Imperial Feet
This platform operates under a strict opinionated assumption that **all depth and length measurements** (Measured Depth, TVD, offsets) are in **Imperial Feet**.

* **No Schema Enforcement:** The database columns utilize standard `REAL` or `DOUBLE` types and do not enforce unit consistency at the database level.
* **Trajectory Calculation:** The `Trajectory Manager` includes a hardcoded conversion factor (`* 0.3048`) when projecting offsets to UTM coordinates. Ingesting metric directional surveys without prior conversion will result in calculated wellbore geometries being approximately **3.28x smaller than reality**.
* **Requirement:** Ensure all depth-based input data is converted to feet prior to ingestion.

### 2. Z-Axis Orientation (GIS vs. Driller's Depth)
There is a fundamental difference between how drilling data and GIS systems handle vertical orientation.

* **Driller's Depth:** In database columns such as `depth_md` and `depth_tvd`, positive values indicate **depth downwards** from the Kelly Bushing (KB).
* **GIS Geometry:** In the `trajectory_geom` column (PostGIS 3D LineString), the Z-axis represents **True Elevation**.
    * **Up** is Positive.
    * **Down** is Negative.
* **Result:** Most subsurface well geometries will contain **Negative Z** values, representing Subsea True Vertical Depth (SSTVD).

### 3. Data Integrity: Zero vs. Null
The `Ingest Manager` explicitly distinguishes between `0` and `NULL` to maintain engineering integrity.

* **0:** Indicates a gauge was active but recorded zero flow.
* **NULL:** Indicates missing data or a broken gauge.
* **Requirement:** If source CSVs use blank strings or other placeholders for zero, they must be cleaned prior to ingestion to prevent them from being stored as `NULL`.

---

## Architecture Modules

The database is architected into four distinct modules. The **Core** module is mandatory; all other modules are optional plugins that may be installed based on requirements.

### 1. Core (Mandatory)
* **Script:** `01_build_core.sql`
* **Function:** Establishes the `Well` to `Wellbore` hierarchy, stores Surface Locations (Lat/Lon), and houses Directional Surveys.
* **Dependencies:** Requires the `PostGIS` extension.

### 2. Production (Optional)
* **Script:** `02_build_production.sql`
* **Function:** Stores Monthly and Daily production data.
* **Feature:** Utilizes **Time-Series Partitioning** (automatic yearly tables) to ensure high performance with millions of rows.

### 3. Geology (Optional)
* **Script:** `03_build_geology.sql`
* **Function:** Stores Formation Tops, Stratigraphy Dictionaries, Rock Samples (Core/Cuttings), and Media/Curve Catalogs.
* **Feature:** Implements **Fault Handling** logic for repeated stratigraphy via an `occurrence` column.

### 4. Completions (Optional)
* **Script:** `04_build_completions.sql`
* **Function:** Stores Frac stages, Perforations, and Stimulation data.

---

## Implementation Guide

### Step 1: Environment Setup
The platform requires Python 3.9+ and PostgreSQL 13+ with PostGIS installed.

**1. Create a Python Environment**
```bash
conda create -n subsurface python=3.10
conda activate subsurface
pip install pandas sqlalchemy psycopg2-binary geoalchemy2 pyproj lasio python-dotenv
```

**2. Configure Environment Variables**

Create a `.env` file in the project root directory:

```ini
DB_URL=postgresql+psycopg2://postgres:password@localhost/subsurface_db
```

### Step 2: Build the Database
Execute the SQL scripts in the following order.

```bash
# Mandatory Core Build
psql -d subsurface_db -f 01_build_core.sql

# Optional Modules (Install as needed)
psql -d subsurface_db -f 02_build_production.sql
psql -d subsurface_db -f 03_build_geology.sql
psql -d subsurface_db -f 04_build_completions.sql
```

### Step 3: Configure Mappings
Edit `field_mapping.json` to map incoming CSV headers to the database schema.

**Example Configuration:**

```json
{
    "well_header_mappings": {
        "uwi": ["API", "UWI", "WELL_ID"],
        "well_name": ["WellName", "LeaseName"],
        "lat": ["SurfaceLat", "LATITUDE"],
        "lon": ["SurfaceLon", "LONGITUDE"],
        "elevation": ["KB", "Elev", "KellyBushing"]
    },
    "tops_mappings": {
        "uwi": ["API", "UWI"],
        "formation": ["Formation", "Zone"],
        "depth": ["MD", "TopDepth"]
    }
}
```

---

## Python Tooling Usage

### 1. Ingest Manager (`ingest_manager.py`)
Responsible for loading CSV data. It performs a dependency check to ensure optional tables exist before attempting ingestion.

*   **Lakehouse Feature:** Any CSV column not found in the mapping file is automatically captured into a `JSONB` attributes column, ensuring no data loss.

**Usage:**

```python
from ingest_manager import SubsurfaceIngestor
import os

db_url = os.getenv("DB_URL")
ingestor = SubsurfaceIngestor(db_url, "field_mapping.json")

# 1. Load Headers (Required First)
ingestor.ingest_headers_csv("raw_data/well_headers.csv")

# 2. Load Modules (Skipped if DB table does not exist)
ingestor.ingest_tops_csv("raw_data/geo_tops.csv")
ingestor.ingest_daily_production_csv("raw_data/prod_daily.csv")
```

### 2. Trajectory Manager (`trajectory_manager.py`)
Calculates the 3D wellbore path using the Minimum Curvature Method.

*   **Trigger:** Run this process after loading Directional Surveys.
*   **Physics Engine:**
    *   Calculates TVD (True Vertical Depth).
    *   Generates a GIS-compliant `LINESTRING Z` geometry.
*   **Auto-Sync:** If the Geology module is installed, it automatically recalculates the TVD for all Formation Tops in the well based on the updated survey.

**Usage:**

```python
from trajectory_manager import TrajectoryProcessor
import os

tp = TrajectoryProcessor(os.getenv("DB_URL"))

# Calculate specific well (by Wellbore UUID)
tp.calculate_well("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
```

### 3. Media Loader (`media_loader.py`)
Scans a directory structure, infers metadata from filenames, and links files to specific wells.

*   **Smart Match:** Utilizes regex to ignore delimiters. `Well-123-A` in a filename will match `Well123A` in the database.
*   **Context Inference:** Categorizes files as "Core Photo", "Thin Section", or "Log" based on file naming conventions.

**Usage:**

```python
from media_loader import MediaLoader
import os

loader = MediaLoader(os.getenv("DB_URL"))
loader.scan_directory("Z:/Shared/Well_Data/Images")
```

---

## Schema Diagram

```mermaid
erDiagram
    well_master ||--|{ wellbore_master : "has"
    wellbore_master ||--|{ directional_surveys : "defines path"
    wellbore_master ||--|{ formation_tops : "intersects"
    wellbore_master ||--|{ production_daily : "produces"
    wellbore_master ||--|{ rock_samples : "extracted from"

    well_master {
        uuid well_id PK
        string uwi "Unique ID"
        geometry surface_geom "Lat/Lon"
        jsonb attributes "Flex Data"
    }

    wellbore_master {
        uuid wellbore_id PK
        geometry trajectory_geom "3D Path"
        real total_depth_tvd
    }

    formation_tops {
        uuid top_id PK
        real depth_md
        real depth_tvd "Calc from Survey"
        int occurrence "Fault logic"
    }
```