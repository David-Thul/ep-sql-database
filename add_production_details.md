# How to Deploy `add_production.sql`

This document outlines how to properly execute the `add_production.sql` script, which uses `\set` (a `psql` client-side command) and a `DO $$` block (server-side PL/pgSQL). Because of this combination, you must run the script using a tool that supports `psql`-style variable substitution.

## Option A: Command Line (The Robust Way)

The most reliable method is to run the script directly from the command line using `psql`.

```bash
psql -d subsurface_db -f add_production.sql
```

This command will correctly interpret the `\set` command and execute the script as intended.

## Option B: Python / SQLAlchemy

If you are executing the SQL from a Python script (e.g., using SQLAlchemy) where `psql` is not involved, the `\set` command will not be recognized. In this scenario, you must hardcode the variable directly within the `DO` block.

Modify the `DECLARE` section at the top of the `add_production.sql` script as follows:

```sql
DO $$
DECLARE
    -- USER CONFIGURATION HERE
    v_start_date DATE := '2015-01-01'; -- <--- CHANGE THIS DATE FREELY
    
    -- Logic follows...
    v_current_year INT := EXTRACT(YEAR FROM CURRENT_DATE);
    -- ...
```