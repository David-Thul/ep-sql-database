# Subsurface PostgreSQL Data Lakehouse

This repository provides a complete framework for building a robust subsurface data lakehouse using PostgreSQL and PostGIS. It includes schema definitions, data ingestion scripts, and a flexible mapping system to handle various data sources.

## Project Overview

The goal is to create a centralized, spatially-enabled database that can store a wide range of subsurface data, from well headers and logs to detailed geological and petrophysical analyses. The system is designed to be modular and extensible.

-   **Database**: PostgreSQL with the PostGIS extension for spatial capabilities.
-   **Data Ingestion**: Python scripts using `SQLAlchemy` and `pandas` for processing and loading data.
-   **Environment**: Managed via Conda (`subsurface_env.yml`).
-   **Visualization**: Easily connects to GIS software like QGIS.

---

## Workflow at a Glance

1.  **Prerequisites**: Install PostgreSQL and Conda.
2.  **Environment Setup**: Create the Python environment using the provided `subsurface_env.yml` file.
3.  **Database Creation**: Set up the PostgreSQL database and run the SQL scripts in the correct order to build the schema.
4.  **Data Ingestion**: Configure `field_mapping.json` and run the `ingest_manager.py` script to load well data (headers, tops, logs).
5.  **Media Cataloging**: Run the `media_loader.py` script to scan for and catalog related media files (core photos, reports).
6.  **Visualization**: Connect your GIS application (e.g., QGIS) to the database to view and analyze the data.

---

## 1. Environment Setup

This project uses Conda to manage its Python environment and dependencies.

**Step 1: Create the Conda Environment**
Open your terminal, navigate to the project directory, and run the following command. This will create a new environment named `subsurface_env` and install all required packages.

```bash
conda env create -f subsurface_env.yml
```

**Step 2: Activate the Environment**
Before running any Python scripts, you must activate the environment:

```bash
conda activate subsurface_env
```

---

## 2. Database Setup

The database schema is built using two sequential SQL scripts. You must run them in the specified order.

**Step 1: Create a Database**
Using a PostgreSQL client of your choice (e.g., `psql`, pgAdmin, DBeaver), create a new database. The default name used throughout this project is `subsurface_db`.

**Step 2: Execute SQL Scripts**
Run the following scripts against your newly created database:

1.  **`lakehouse_build.sql`**: This script sets up the core data model, including tables for wells (`well_master`), wellbores, formation tops, and the curve catalog. It also enables the PostGIS extension.

2.  **`add_geology_build.sql`**: This script extends the schema with detailed tables for geological and petrophysical data, such as rock sample analysis, fluid properties, and core descriptions.

---

## 3. Data Ingestion (`ingest_manager.py`)

The `ingest_manager.py` script is the primary tool for loading structured data (CSVs, LAS files) into the database.

### Field Mapping (`field_mapping.json`)

Before ingesting data, you may need to configure `field_mapping.json`. This file tells the ingestor how to map column names from your source files to the fields in the database. This is crucial for handling data from different vendors or regions with varying naming conventions.

The mapping uses a "first match wins" logic. For a given database field, the ingestor will scan the source file's columns for the first name in the alias list that it finds.

### Running the Ingestor

The `ingest_manager.py` script contains functions to process:
-   **Well Headers**: `ingest_headers_csv()`
-   **Formation Tops**: `ingest_tops_csv()`
-   **LAS Files**: `ingest_las_file()`

You can uncomment and run these functions from the `if __name__ == "__main__":` block at the bottom of the script.

---

## 4. Media Cataloging (`media_loader.py`)

The `media_loader.py` script scans directories for unstructured data like images, PDFs, and other documents. It uses regular expressions to find a Unique Well ID (UWI) in the file or folder path and extracts other metadata (like depth) from the filename.

It populates the `media_catalog` table with a reference to the file's path, but **does not** move or copy the file itself. This creates a searchable index of your media assets within the database.

For more details on its operation and configuration, see `media_loader_details.md`.

---

## 5. Visualization in QGIS

Once your data is loaded, you can easily connect QGIS to your PostgreSQL database.

**Prerequisites**:
*   QGIS version 3.34+ or a recent Long-Term Release (LTR).

### Connection Steps

1.  **Open QGIS**.
2.  In the **Browser Panel**, right-click on **PostgreSQL** and select **New Connection...**.
3.  Enter the connection details:
    *   **Name**: A descriptive name, e.g., `Subsurface Lakehouse`
    *   **Host**: `localhost` (or your database server's IP)
    *   **Database**: `subsurface_db`
    *   **Authentication**: Enter your PostgreSQL username and password.
4.  Click **Test Connection** to verify, then click **OK** to save.

### Loading Layers

Once connected, expand the connection in the Browser Panel, open the `public` schema, and drag tables like `well_master` onto the map canvas to visualize them.


