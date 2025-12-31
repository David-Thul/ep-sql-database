import pandas as pd
import json
import os
import lasio
import numpy as np
from typing import Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load env variables
load_dotenv()

# --- CONFIGURATION ---
# Get URL safely
DB_CONNECTION_STR = os.getenv("DB_URL")
CONFIG_FILE = "field_mapping.json"
LAKE_STORE_PATH = "./lake_data_parquet"

class SubsurfaceIngestor:
    def __init__(self, db_url: str, config_file: str, capture_unknowns: bool = True):
        """
        :param capture_unknowns: If True, unmapped CSV columns are saved to the 'attributes' JSONB column.
        """
        self.engine: Engine = create_engine(db_url)
        self.capture_unknowns = capture_unknowns
        with open(config_file, 'r') as f:
            self.mappings: dict[str, dict[str, list[str]]] = json.load(f)

    def _process_dataframe(self, df: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
        """
        Normalizes mapped columns and optionally captures unmapped ones.
        """
        target_map = self.mappings[mapping_key]
        rename_map = {}
        mapped_source_cols = set()

        # 1. Build the rename map (CSV Header -> DB Column)
        for target_col, aliases in target_map.items():
            upper_aliases = {a.upper() for a in aliases}
            for csv_col in df.columns:
                if csv_col.upper() in upper_aliases:
                    rename_map[csv_col] = target_col
                    mapped_source_cols.add(csv_col)
                    break # First match wins for this target column
        
        # 2. Rename the known columns
        df_clean = df.rename(columns=rename_map)
        
        # 3. Handle Unknowns (attributes)
        if self.capture_unknowns:
            # Identify columns that were NOT mapped
            unknown_cols = [c for c in df.columns if c not in mapped_source_cols]
            
            if unknown_cols:
                # Convert these columns to a list of dicts: [{'Rig': 'R1'}, {'Rig': 'R2'}]
                # We use apply to create a JSON-compatible dict for every row
                print(f"   -> Capturing {len(unknown_cols)} unknown columns into JSONB.")
                df_clean['attributes'] = df[unknown_cols].to_dict(orient='records')
            else:
                df_clean['attributes'] = [{}] * len(df) # Empty dicts if no unknowns
        else:
            df_clean['attributes'] = [{}] * len(df)

        # 4. Filter: Keep only Mapped Columns + 'attributes'
        valid_cols = list(target_map.keys()) + ['attributes']
        # strictly keep only columns that exist in our clean dataframe
        existing_cols = [c for c in valid_cols if c in df_clean.columns]
        
        return df_clean[existing_cols]

    def ingest_headers_csv(self, csv_path: str) -> None:
        """Loads well headers with optional JSONB capture."""
        print(f"🔹 Processing Header File: {csv_path}")
        
        df = pd.read_csv(csv_path, dtype=str) 
        
        # Normalize & Capture
        df = self._process_dataframe(df, "well_header_mappings")
        
        # Clean UWI
        df['uwi'] = df['uwi'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Serialize dict to JSON string if necessary, 
                # but SQLAlchemy handles dict->jsonb automatically with psycopg2.
                attrs = row.get('attributes', {})

                sql = text("""
                    INSERT INTO well_master (uwi, well_name, operator, surface_geom, attributes)
                    VALUES (:uwi, :name, :op, ST_SetSRID(ST_MakePoint(:lon::numeric, :lat::numeric), 4269), :attrs)
                    ON CONFLICT (uwi) DO UPDATE 
                    SET well_name = EXCLUDED.well_name, 
                        operator = EXCLUDED.operator,
                        attributes = well_master.attributes || EXCLUDED.attributes 
                    RETURNING well_id;
                """)
                # Note on Update: '||' merges the new JSON attributes with existing ones!

                if pd.isna(row.get('lat')) or pd.isna(row.get('lon')):
                    print(f"Skipping {row['uwi']} - No coordinates")
                    continue

                conn.execute(sql, {
                    "uwi": row['uwi'],
                    "name": row.get('well_name'),
                    "op": row.get('operator'),
                    "lon": row.get('lon'),
                    "lat": row.get('lat'),
                    "attrs": json.dumps(attrs) # Explicit dump ensures format
                })
                
                # Ensure Default Wellbore
                # We need the ID we just inserted/updated to link the wellbore
                # Re-fetching ID to be safe or using logic to get it from result is tricky in bulk loops without return
                # Simpler: Just get ID by UWI for the wellbore insert
                conn.execute(text("""
                    INSERT INTO wellbore_master (well_id, wellbore_name)
                    SELECT well_id, 'OH' FROM well_master WHERE uwi = :uwi
                    ON CONFLICT DO NOTHING
                """), {"uwi": row['uwi']})

        print("✅ Headers Loaded.")

    def ingest_tops_csv(self, csv_path: str) -> None:
        """Loads formation tops."""
        print(f"🔹 Processing Tops File: {csv_path}")
        df = pd.read_csv(csv_path)
        # Note: We typically don't store unknown cols for Tops, but you could add logic here similarly
        df = self._process_dataframe(df, "tops_mappings")
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # 1. Get Wellbore ID
                wb_res = conn.execute(text("""
                    SELECT wb.wellbore_id FROM wellbore_master wb 
                    JOIN well_master w ON wb.well_id = w.well_id 
                    WHERE w.uwi = :uwi
                """), {"uwi": row['uwi']}).scalar()

                if not wb_res:
                    continue 

                # 2. Get/Create Strat ID
                strat_id = conn.execute(text("""
                    WITH ins AS (
                        INSERT INTO strat_unit_dictionary (unit_name) VALUES (:fm)
                        ON CONFLICT (unit_name) DO NOTHING RETURNING strat_unit_id
                    )
                    SELECT strat_unit_id FROM ins
                    UNION ALL
                    SELECT strat_unit_id FROM strat_unit_dictionary WHERE unit_name = :fm
                    LIMIT 1
                """), {"fm": row['formation']}).scalar()

                # 3. Insert Top
                conn.execute(text("""
                    INSERT INTO formation_tops (wellbore_id, strat_unit_id, depth_md)
                    VALUES (:wb, :sid, :md)
                """), {"wb": wb_res, "sid": strat_id, "md": row['depth']})
        
        print("✅ Tops Loaded.")

    def ingest_las_file(self, las_path: str) -> None:
        # (Same as previous version - LAS logic is distinct from CSV mapping)
        print(f"🔹 Processing LAS: {las_path}")
        try:
            las = lasio.read(las_path)
        except Exception as e:
            print(f"❌ Failed: {e}")
            return

        uwi = str(las.well.API.value).replace('-', '').strip()
        if not uwi: return

        df_curves = las.df().reset_index()
        os.makedirs(LAKE_STORE_PATH, exist_ok=True)
        filename = f"{uwi}_{os.path.basename(las_path)}.parquet"
        save_path = os.path.join(LAKE_STORE_PATH, filename)
        df_curves.to_parquet(save_path, index=False, engine='pyarrow', compression='snappy')

        with self.engine.begin() as conn:
            wb_id = conn.execute(text("""
                SELECT wb.wellbore_id FROM wellbore_master wb 
                JOIN well_master w ON wb.well_id = w.well_id 
                WHERE w.uwi = :uwi
            """), {"uwi": uwi}).scalar()

            if wb_id:
                curve_names = list(df_curves.columns)
                min_d = float(df_curves.iloc[:, 0].min())
                max_d = float(df_curves.iloc[:, 0].max())

                conn.execute(text("""
                    INSERT INTO curve_catalog 
                    (wellbore_id, file_path, channels, min_depth, max_depth, dataset_name)
                    VALUES (:wb, :path, :chans, :min_d, :max_d, 'Imported LAS')
                """), {
                    "wb": wb_id, "path": save_path, "chans": curve_names,
                    "min_d": min_d, "max_d": max_d
                })
                print(f"✅ Registered Curves for {uwi}")

# --- RUNNER ---
if __name__ == "__main__":
    # Toggle capture_unknowns=True to save extra CSV columns to JSON
    ingestor = SubsurfaceIngestor(DB_CONNECTION_STR, CONFIG_FILE, capture_unknowns=True)
    
    # ingestor.ingest_headers_csv("your_data.csv")