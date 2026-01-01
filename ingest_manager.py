import pandas as pd
import json
import os
import lasio
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

# Load env variables
load_dotenv()

DB_CONNECTION_STR = os.getenv("DB_URL")
CONFIG_FILE = "field_mapping.json"
LAKE_STORE_PATH = "./lake_data_parquet"

class SubsurfaceIngestor:
    def __init__(self, db_url: str, config_file: str, capture_unknowns: bool = True):
        if not db_url:
            raise ValueError("DB_URL is not set. Check your environment variables.")
            
        self.engine: Engine = create_engine(db_url)
        self.capture_unknowns = capture_unknowns
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.mappings = json.load(f)
        else:
            print(f"⚠️ Warning: Config file {config_file} not found. Mappings will be empty.")
            self.mappings = {}

    def _check_dependency(self, table_name: str) -> bool:
        """Returns True if the optional module (table) exists."""
        return inspect(self.engine).has_table(table_name)

    def _process_dataframe(self, df: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
        """Normalizes columns based on config and captures extra data."""
        if mapping_key not in self.mappings:
            return df

        target_map = self.mappings[mapping_key]
        rename_map = {}
        mapped_source_cols = set()

        # 1. Build Rename Map
        for target_col, aliases in target_map.items():
            upper_aliases = {str(a).upper() for a in aliases}
            for csv_col in df.columns:
                if str(csv_col).upper() in upper_aliases:
                    rename_map[csv_col] = target_col
                    mapped_source_cols.add(csv_col)
                    break 
        
        df_clean = df.rename(columns=rename_map)
        
        # 2. Capture Unmapped Columns (Lakehouse Feature)
        if self.capture_unknowns:
            unknown_cols = [c for c in df.columns if c not in mapped_source_cols]
            if unknown_cols:
                # Convert to string to avoid serialization errors
                df_clean['attributes'] = df[unknown_cols].astype(str).to_dict(orient='records')
            else:
                df_clean['attributes'] = [{} for _ in range(len(df))]
        else:
            df_clean['attributes'] = [{} for _ in range(len(df))]

        # Return only valid columns + attributes
        valid_cols = list(target_map.keys()) + ['attributes']
        existing_cols = [c for c in valid_cols if c in df_clean.columns]
        return df_clean[existing_cols]

    def ingest_headers_csv(self, csv_path: str) -> None:
        """Core Module: Always runs. Ingests Surface Locations."""
        print(f"🔹 Processing Header File: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)
        df = self._process_dataframe(df, "well_header_mappings")
        
        # Cleanup
        df['uwi'] = df['uwi'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        # Fix: Use specific lat/lon columns or generic ones if mapped
        if 'lat' in df.columns: df['lat_surface'] = pd.to_numeric(df['lat'], errors='coerce')
        if 'lon' in df.columns: df['lon_surface'] = pd.to_numeric(df['lon'], errors='coerce')
        if 'elevation' in df.columns: df['elevation_kb'] = pd.to_numeric(df['elevation'], errors='coerce')
        
        df = df.dropna(subset=['uwi'])

        records = df.to_dict(orient='records')
        
        # Upsert Logic
        upsert_sql = text("""
            INSERT INTO well_master (uwi, well_name, operator, surface_geom, lat_surface, lon_surface, elevation_kb, attributes)
            VALUES (:uwi, :well_name, :operator, 
                    ST_SetSRID(ST_MakePoint(:lon_surface, :lat_surface), 4269), 
                    :lat_surface, :lon_surface, :elevation_kb,
                    :attributes)
            ON CONFLICT (uwi) DO UPDATE 
            SET well_name = EXCLUDED.well_name, 
                operator = EXCLUDED.operator,
                elevation_kb = COALESCE(EXCLUDED.elevation_kb, well_master.elevation_kb),
                attributes = well_master.attributes || EXCLUDED.attributes;
        """)

        with self.engine.begin() as conn:
            print(f"   -> Upserting {len(records)} wells...")
            # We filter records to match the parameters expected by the SQL
            for row in records:
                # Ensure all keys exist with default None
                params = {
                    "uwi": row.get('uwi'),
                    "well_name": row.get('well_name'),
                    "operator": row.get('operator'),
                    "lat_surface": row.get('lat_surface'),
                    "lon_surface": row.get('lon_surface'),
                    "elevation_kb": row.get('elevation_kb'),
                    "attributes": json.dumps(row.get('attributes', {}))
                }
                conn.execute(upsert_sql, params)
                
                # Ensure 'OH' wellbore exists
                wb_sql = text("""
                    INSERT INTO wellbore_master (well_id, wellbore_name)
                    SELECT well_id, 'OH' FROM well_master WHERE uwi = :uwi
                    ON CONFLICT DO NOTHING
                """)
                conn.execute(wb_sql, {"uwi": row['uwi']})

        print("✅ Headers Loaded.")

    def ingest_tops_csv(self, csv_path: str) -> None:
        """Geology Module: Checks for 'formation_tops'."""
        if not self._check_dependency('formation_tops'):
            print("   ⚠️ Skipping Tops: Geology module (formation_tops) not installed.")
            return

        print(f"🔹 Processing Tops File: {csv_path}")
        df = pd.read_csv(csv_path)
        df = self._process_dataframe(df, "tops_mappings")
        
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['depth'] = pd.to_numeric(df['depth'], errors='coerce')
        df = df.dropna(subset=['uwi', 'formation', 'depth'])
        
        # Fault Handling: Sort and Count
        df = df.sort_values(by=['uwi', 'formation', 'depth'])
        if 'interpreter' not in df.columns: df['interpreter'] = 'Unknown'
        df['interpreter'] = df['interpreter'].fillna('Unknown')
        df['occurrence'] = df.groupby(['uwi', 'formation', 'interpreter']).cumcount() + 1

        with self.engine.begin() as conn:
            # 1. Strat Dictionary
            unique_fms = df['formation'].unique().tolist()
            conn.execute(text("INSERT INTO strat_unit_dictionary (unit_name) VALUES (:n) ON CONFLICT DO NOTHING"), 
                         [{"n": f} for f in unique_fms])
            
            # 2. Get IDs (Simplified one-by-one for robustness in this snippet)
            # For high volume, use bulk dictionary lookup
            for _, row in df.iterrows():
                wb_id = conn.execute(text("SELECT wb.wellbore_id FROM wellbore_master wb JOIN well_master w ON wb.well_id = w.well_id WHERE w.uwi = :uwi LIMIT 1"), {"uwi": row['uwi']}).scalar()
                strat_id = conn.execute(text("SELECT strat_unit_id FROM strat_unit_dictionary WHERE unit_name = :n"), {"n": row['formation']}).scalar()
                
                if wb_id and strat_id:
                    conn.execute(text("""
                        INSERT INTO formation_tops (wellbore_id, strat_unit_id, depth_md, interpreter, pick_quality, occurrence)
                        VALUES (:wb, :sid, :md, :interp, :qual, :occ)
                        ON CONFLICT (wellbore_id, strat_unit_id, interpreter, occurrence) 
                        DO UPDATE SET depth_md = EXCLUDED.depth_md
                    """), {
                        "wb": wb_id, "sid": strat_id, "md": row['depth'],
                        "interp": row['interpreter'], "qual": row.get('quality'), "occ": row['occurrence']
                    })

        print("✅ Tops Loaded.")

    def ingest_daily_production_csv(self, csv_path: str) -> None:
        """Production Module: Checks for 'production_daily'."""
        if not self._check_dependency('production_daily'):
            print("   ⚠️ Skipping Production: Production module not installed.")
            return

        print(f"🔹 Processing Daily Production: {csv_path}")
        df = pd.read_csv(csv_path)
        df = self._process_dataframe(df, "daily_mappings")
        
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['uwi', 'date'])

        # Numeric Safety (Keep NULL if missing, don't force 0 for Engineering reasons)
        numeric_cols = ['oil', 'gas', 'water', 'hours_on', 'tubing_pressure', 'casing_pressure', 'choke_size']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce') # Keeps NaN
            else:
                df[col] = None

        with self.engine.begin() as conn:
            # Batch this for speed
            records = df.to_dict(orient='records')
            
            # Pre-fetch wellbore map for speed
            uwi_list = tuple(df['uwi'].unique())
            if not uwi_list: return
            
            # Handle single item tuple syntax
            if len(uwi_list) == 1: 
                query_str = "SELECT w.uwi, wb.wellbore_id FROM wellbore_master wb JOIN well_master w ON wb.well_id = w.well_id WHERE w.uwi = :uwi"
                wb_res = conn.execute(text(query_str), {"uwi": uwi_list[0]}).fetchall()
            else:
                query_str = "SELECT w.uwi, wb.wellbore_id FROM wellbore_master wb JOIN well_master w ON wb.well_id = w.well_id WHERE w.uwi IN :uwis"
                wb_res = conn.execute(text(query_str), {"uwis": uwi_list}).fetchall()
                
            wb_map = {row[0]: row[1] for row in wb_res}

            final_rows = []
            for r in records:
                if r['uwi'] in wb_map:
                    final_rows.append({
                        "wb": wb_map[r['uwi']],
                        "dt": r['date'],
                        "oil": r.get('oil'), "gas": r.get('gas'), "wtr": r.get('water'),
                        "hrs": r.get('hours_on'), "thp": r.get('tubing_pressure'),
                        "chp": r.get('casing_pressure'), "choke": r.get('choke_size'),
                        "attr": json.dumps(r.get('attributes', {}))
                    })
            
            if final_rows:
                print(f"   -> Inserting {len(final_rows)} rows...")
                conn.execute(text("""
                    INSERT INTO production_daily 
                    (wellbore_id, prod_date, oil_vol, gas_vol, water_vol, 
                     hours_on, tubing_pressure, casing_pressure, choke_size, attributes)
                    VALUES 
                    (:wb, :dt, :oil, :gas, :wtr, :hrs, :thp, :chp, :choke, :attr)
                    ON CONFLICT (wellbore_id, prod_date) 
                    DO UPDATE SET 
                        oil_vol = EXCLUDED.oil_vol,
                        gas_vol = EXCLUDED.gas_vol,
                        hours_on = EXCLUDED.hours_on,
                        tubing_pressure = EXCLUDED.tubing_pressure
                """), final_rows)

        print("✅ Daily Production Loaded.")

if __name__ == "__main__":
    if not DB_CONNECTION_STR:
        print("❌ Error: DB_URL environment variable not found.")
    else:
        ingestor = SubsurfaceIngestor(DB_CONNECTION_STR, CONFIG_FILE)
        # ingestor.ingest_headers_csv("data/headers.csv")