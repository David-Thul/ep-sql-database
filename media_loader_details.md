# Media Loader Details

This document provides details on the execution, best practices, and database interactions of the `MediaLoader`.

## 1. Basic Execution

The loader requires an active SQLAlchemy engine and a root directory path to scan for media files.

```python
from sqlalchemy import create_engine
from media_loader import MediaLoader
import os

# Connect to the Database
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:password@localhost/subsurface_db")
engine = create_engine(DB_URL)

# Initialize and Run the Loader
loader = MediaLoader(engine)
loader.scan_directory("/mnt/data/server/Core_Photos")
```

## 2. File Naming Best Practices

While the regex engine is robust, adhering to these naming conventions maximizes data extraction accuracy.

**Optimal Pattern:** `{UWI}_{Type}_{Top}-{Base}.{ext}`

| Component     | Recommended Format              | Example                  |
|---------------|---------------------------------|--------------------------|
| **UWI**       | 10-14 digits                    | `4230134555`             |
| **Depth Range** | Hyphen or underscore separated  | `3500-3510` or `3500_to_3510` |
| **Single Depth**| Number followed by 'ft' or 'm'  | `4500.5ft`               |
| **Light Source**| UV, White, PPL, XPL             | `UV`, `White_Light`, `XPL` |

---

### Examples of Successfully Parsed Paths:

*   **Path:** `D:/Data/4230130001/Core/Box3_4500-4510_UV.jpg`
    *   **Result:** Linked to UWI `4230130001`, Type: `Core Photo (UV)`, Top: `4500`, Base: `4510`.

*   **Path:** `D:/Data/Projects/Permian/Plug_3200.5ft.tif` (Assuming parent folder has UWI)
    *   **Result:** Linked to Parent UWI, Type: `General`, Top: `3200.5`, Base: `3200.5`.

*   **Path:** `D:/Images/42-105-99999_ThinSection_XPL.jpg`
    *   **Result:** Linked to UWI `4210599999`, Type: `Thin Section (XPL)`.

---

## 3. Database Target

The loader populates the `media_catalog` table. No physical files are moved or renamed; only their metadata and pointers are stored in the database.

### Table Schema

```sql
TABLE media_catalog (
    media_id        UUID PRIMARY KEY,
    wellbore_id     UUID FK,
    media_type      VARCHAR(50),  -- e.g., 'Core Photo', 'SEM'
    file_path       TEXT,         -- Absolute path to file
    top_depth_md    REAL,
    base_depth_md   REAL,
    description     TEXT,         -- Auto-generated tag (e.g., 'UV Light | Source: Box3')
    created_at      TIMESTAMP
);
```

### SQL Query Examples

**Find all UV Core Photos for a specific Formation:**
*(Requires `formation_tops` to be populated)*

```sql
SELECT
    w.well_name,
    m.top_depth_md,
    m.file_path
FROM
    media_catalog m
JOIN
    wellbore_master wb ON m.wellbore_id = wb.wellbore_id
JOIN
    well_master w ON wb.well_id = w.well_id
JOIN
    formation_tops t ON t.wellbore_id = wb.wellbore_id
JOIN
    strat_unit_dictionary s ON t.strat_unit_id = s.strat_unit_id
WHERE
    s.unit_name = 'Eagle Ford'
    AND m.media_type = 'Core Photo'
    AND m.description ILIKE '%UV%'
    AND m.top_depth_md >= t.depth_md;
```