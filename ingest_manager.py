import pandas as pd
import json
import os
import lasio
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load env variables (Ensure DB_URL is in your .env file)
load_dotenv()

DB_CONNECTION_STR = os.getenv("DB_URL")
CONFIG_FILE = "field_mapping.json"
LAKE_STORE_PATH = "./lake_data_parquet"

class SubsurfaceIngestor:
    def __init__(self, db_url: str, config_file: str, capture_unknowns: bool = True):
        """
        :param db_url: SQLAlchemy connection string.
        :param config_file: Path to field_mapping.json.
        :param capture_unknowns: If True, unmapped CSV columns go into 'attributes' JSONB.
        """
        if not db_url:
            raise ValueError("DB_URL is not set. Check your environment variables.")
            
        self.engine: Engine = create_engine(db_url)
        self.capture_unknowns = capture_unknowns
        
        if not os.path.exists(config_file):
            print(f"⚠️ Warning: Config file {config_file} not found. Mappings will be empty.")
            self.mappings = {}
        else:
            with open(config_file, 'r') as f:
                self.mappings: dict[str, dict[str, list[str]]] = json.load(f)

    def _process_dataframe(self, df: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
        """
        Normalizes mapped columns and optionally captures unmapped ones into a JSON column.
        """
        if mapping_key not in self.mappings:
            print(f"❌ Mapping key '{mapping_key}' not found in config.")
            return df

        target_map = self.mappings[mapping_key]
        rename_map = {}
        mapped_source_cols = set()

        # 1. Build Rename Map based on Config
        for target_col, aliases in target_map.items():
            upper_aliases = {a.upper() for a in aliases}
            for csv_col in df.columns:
                if csv_col.upper() in upper_aliases:
                    rename_map[csv_col] = target_col
                    mapped_source_cols.add(csv_col)
                    break 
        
        df_clean = df.rename(columns=rename_map)
        
        # 2. Capture Unmapped Columns (The "Lakehouse" Feature)
        if self.capture_unknowns:
            unknown_cols = [c for c in df.columns if c not in mapped_source_cols]
            if unknown_cols:
                print(f"   -> Capturing {len(unknown_cols)} unknown columns into 'attributes'.")
                # Convert to string to avoid serialization issues with dates/NaNs
                df_clean['attributes'] = df[unknown_cols].astype(str).to_dict(orient='records')
            else:
                df_clean['attributes'] = [{} for _ in range(len(df))]
        else:
            df_clean['attributes'] = [{} for _ in range(len(df))]

        # 3. Return only valid columns + attributes
        valid_cols = list(target_map.keys()) + ['attributes']
        existing_cols = [c for c in valid_cols if c in df_clean.columns]
        return df_clean[existing_cols]

    def _get_id_map(self, conn, table, key_col, id_col, keys):
        """
        Efficiently fetches {key: id} for a list of keys (Bulk Lookup).
        """
        if not keys: return {}
        # Simple fetch (for production, consider chunking if >50k keys)
        query = text(f"SELECT {key_col}, {id_col} FROM {table} WHERE {key_col} IN :keys")
        result = conn.execute(query, {"keys": tuple(keys)}).fetchall()
        return {row[0]: row[1] for row in result}

    def ingest_headers_csv(self, csv_path: str) -> None:
        """
        Ingests Well Headers (Surface Locations).
        """
        print(f"🔹 Processing Header File: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)
        df = self._process_dataframe(df, "well_header_mappings")
        
        # Cleanup
        df['uwi'] = df['uwi'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        
        # Critical Data Check
        df = df.dropna(subset=['uwi'])

        records = df.to_dict(orient='records')
        
        # Bulk Upsert (Update if exists, Insert if new)
        upsert_sql = text("""
            INSERT INTO well_master (uwi, well_name, operator, surface_geom, attributes)
            VALUES (:uwi, :well_name, :operator, 
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4269), 
                    :attributes)
            ON CONFLICT (uwi) DO UPDATE 
            SET well_name = EXCLUDED.well_name, 
                operator = EXCLUDED.operator,
                attributes = well_master.attributes || EXCLUDED.attributes;
        """)

        with self.engine.begin() as conn:
            print(f"   -> Bulk upserting {len(records)} wells...")
            conn.execute(upsert_sql, records)

            # Ensure default 'OH' wellbores exist for these wells
            uwi_list = df['uwi'].unique().tolist()
            well_map = self._get_id_map(conn, 'well_master', 'uwi', 'well_id', uwi_list)
            
            wb_records = []
            for uwi in uwi_list:
                if uwi in well_map:
                    wb_records.append({"well_id": well_map[uwi], "wb_name": "OH"})
            
            if wb_records:
                conn.execute(text("""
                    INSERT INTO wellbore_master (well_id, wellbore_name)
                    VALUES (:well_id, :wb_name)
                    ON CONFLICT DO NOTHING
                """), wb_records)
                
        print("✅ Headers Loaded.")

    def ingest_tops_csv(self, csv_path: str) -> None:
        """
        Ingests Formation Tops (Geology).
        Handles Faults/Repeated Sections via 'occurrence'.
        """
        print(f"🔹 Processing Tops File: {csv_path}")
        df = pd.read_csv(csv_path)
        df = self._process_dataframe(df, "tops_mappings")
        
        # Standard Cleanup
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['formation'] = df['formation'].str.strip()
        df['depth'] = pd.to_numeric(df['depth'], errors='coerce')
        df = df.dropna(subset=['uwi', 'formation', 'depth'])
        
        # --- FAULT HANDLING LOGIC ---
        # 1. Sort by UWI, Formation, Depth (Shallow -> Deep)
        df = df.sort_values(by=['uwi', 'formation', 'depth'])
        
        # 2. Default Interpreter if missing
        if 'interpreter' not in df.columns:
            df['interpreter'] = 'Unknown'
        df['interpreter'] = df['interpreter'].fillna('Unknown')
        
        # 3. Calculate Occurrence (1st hit, 2nd hit, etc.)
        df['occurrence'] = df.groupby(['uwi', 'formation', 'interpreter']).cumcount() + 1
        
        if df['occurrence'].max() > 1:
            print(f"   -> ⚠️ Detected {len(df[df['occurrence']>1])} repeated sections (Faults).")

        with self.engine.begin() as conn:
            # 1. Resolve Strat Units (Create dictionary entries if needed)
            unique_fms = df['formation'].unique().tolist()
            conn.execute(text("""
                INSERT INTO strat_unit_dictionary (unit_name)
                VALUES (:unit_name)
                ON CONFLICT (unit_name) DO NOTHING
            """), [{"unit_name": fm} for fm in unique_fms])
            
            strat_map = self._get_id_map(conn, 'strat_unit_dictionary', 'unit_name', 'strat_unit_id', unique_fms)
            
            # 2. Resolve Wellbores
            unique_uwis = df['uwi'].unique().tolist()
            wb_query = text("""
                SELECT w.uwi, wb.wellbore_id 
                FROM wellbore_master wb 
                JOIN well_master w ON wb.well_id = w.well_id 
                WHERE w.uwi IN :uwis
            """)
            wb_res = conn.execute(wb_query, {"uwis": tuple(unique_uwis)}).fetchall()
            wb_map = {row[0]: row[1] for row in wb_res}
            
            # 3. Map IDs
            df['strat_unit_id'] = df['formation'].map(strat_map)
            df['wellbore_id'] = df['uwi'].map(wb_map)
            
            # 4. Filter & Insert
            valid_tops = df.dropna(subset=['wellbore_id', 'strat_unit_id']).copy()
            
            if not valid_tops.empty:
                # Ensure quality exists
                if 'quality' not in valid_tops.columns: valid_tops['quality'] = None

                tops_records = valid_tops[[
                    'wellbore_id', 'strat_unit_id', 'depth', 
                    'interpreter', 'quality', 'occurrence'
                ]].to_dict(orient='records')
                
                print(f"   -> Bulk inserting {len(tops_records)} tops...")
                
                # Insert with Conflict Handling on the new Composite Key
                conn.execute(text("""
                    INSERT INTO formation_tops 
                    (wellbore_id, strat_unit_id, depth_md, interpreter, pick_quality, occurrence)
                    VALUES (:wellbore_id, :strat_unit_id, :depth, :interpreter, :quality, :occurrence)
                    ON CONFLICT (wellbore_id, strat_unit_id, interpreter, occurrence) 
                    DO UPDATE SET depth_md = EXCLUDED.depth_md
                """), tops_records)

        print("✅ Tops Loaded.")

    def ingest_daily_production_csv(self, csv_path: str) -> None:
        """
        Ingests Daily Production Data.
        Requires 'daily_mappings' in field_mapping.json.
        """
        print(f"🔹 Processing Daily Production: {csv_path}")
        df = pd.read_csv(csv_path)
        
        if "daily_mappings" not in self.mappings:
            print("❌ 'daily_mappings' key missing in field_mapping.json. Skipping.")
            return

        df = self._process_dataframe(df, "daily_mappings")
        
        # Cleanup & Standardization
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['uwi', 'date'])

        # Numeric Safety (Convert errors to 0)
        numeric_cols = ['oil', 'gas', 'water', 'hours_on', 'tubing_pressure', 'casing_pressure', 'choke_size']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0 

        with self.engine.begin() as conn:
            # Resolve Wellbores
            unique_uwis = df['uwi'].unique().tolist()
            wb_query = text("""
                SELECT w.uwi, wb.wellbore_id 
                FROM wellbore_master wb 
                JOIN well_master w ON wb.well_id = w.well_id 
                WHERE w.uwi IN :uwis
            """)
            wb_res = conn.execute(wb_query, {"uwis": tuple(unique_uwis)}).fetchall()
            wb_map = {row[0]: row[1] for row in wb_res}
            
            df['wellbore_id'] = df['uwi'].map(wb_map)
            valid_prod = df.dropna(subset=['wellbore_id']).copy()
            
            if not valid_prod.empty:
                # Rename keys to match SQL parameters exactly
                records = valid_prod.rename(columns={
                    'date': 'prod_date',
                    'oil': 'oil_vol',
                    'gas': 'gas_vol',
                    'water': 'water_vol'
                }).to_dict(orient='records')

                print(f"   -> Bulk inserting {len(records)} daily records...")
                
                # Insert into Parent Table (Postgres partitions automatically)
                conn.execute(text("""
                    INSERT INTO production_daily 
                    (wellbore_id, prod_date, oil_vol, gas_vol, water_vol, 
                     hours_on, tubing_pressure, casing_pressure, choke_size, comments)
                    VALUES 
                    (:wellbore_id, :prod_date, :oil_vol, :gas_vol, :water_vol, 
                     :hours_on, :tubing_pressure, :casing_pressure, :choke_size, :attributes)
                    ON CONFLICT (wellbore_id, prod_date) 
                    DO UPDATE SET 
                        oil_vol = EXCLUDED.oil_vol,
                        gas_vol = EXCLUDED.gas_vol,
                        hours_on = EXCLUDED.hours_on,
                        tubing_pressure = EXCLUDED.tubing_pressure
                """), records)

        print("✅ Daily Production Loaded.")

    def ingest_las_file(self, las_path: str) -> None:
        """
        Converts LAS to Parquet and registers it in the Curve Catalog.
        """
        print(f"🔹 Processing LAS: {las_path}")
        try:
            las = lasio.read(las_path)
        except Exception as e:
            print(f"❌ Failed to read LAS: {e}")
            return

        # Attempt to find UWI in Header
        uwi = ""
        # Common locations for UWI in LAS files
        for item in [las.well.API, las.well.UWI]:
            if item and item.value:
                uwi = str(item.value).replace('-', '').strip()
                break
        
        if not uwi: 
            # Fallback: Try filename
            uwi = os.path.basename(las_path).split('.')[0].replace('-', '')
            print(f"   ⚠️ No API in header. Using filename UWI: {uwi}")

        df_curves = las.df().reset_index()
        os.makedirs(LAKE_STORE_PATH, exist_ok=True)
        filename = f"{uwi}_{os.path.basename(las_path)}.parquet"
        save_path = os.path.join(LAKE_STORE_PATH, filename)
        
        # Save to Parquet (High Performance Storage)
        df_curves.to_parquet(save_path, index=False, engine='pyarrow', compression='snappy')

        with self.engine.begin() as conn:
            # Find Wellbore
            wb_id = conn.execute(text("""
                SELECT wb.wellbore_id FROM wellbore_master wb 
                JOIN well_master w ON wb.well_id = w.well_id 
                WHERE w.uwi = :uwi
            """), {"uwi": uwi}).scalar()

            if wb_id:
                curve_names = list(df_curves.columns)
                min_d = float(df_curves.iloc[:, 0].min())
                max_d = float(df_curves.iloc[:, 0].max())

                # Register in Catalog
                conn.execute(text("""
                    INSERT INTO curve_catalog 
                    (wellbore_id, file_path, channels, min_depth, max_depth, dataset_name)
                    VALUES (:wb, :path, :chans, :min_d, :max_d, 'Imported LAS')
                """), {
                    "wb": wb_id, "path": save_path, "chans": curve_names,
                    "min_d": min_d, "max_d": max_d
                })
                print(f"✅ Registered Curves for {uwi}")
            else:
                print(f"   ⚠️ Well {uwi} not found in DB. Curves saved to lake but not linked.")

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    # Ensure DB_URL is available
    if not DB_CONNECTION_STR:
        print("❌ Error: DB_URL environment variable not found.")
    else:
        ingestor = SubsurfaceIngestor(DB_CONNECTION_STR, CONFIG_FILE)
        
        # Examples (Uncomment to run):
        # ingestor.ingest_headers_csv("data/well_headers.csv")
        # ingestor.ingest_tops_csv("data/formation_tops.csv")
        # ingestor.ingest_daily_production_csv("data/daily_production.csv")
        
        # Process a directory of LAS files
        # las_dir = "data/las_files"
        # if os.path.exists(las_dir):
        #     for f in os.listdir(las_dir):
        #         if f.lower().endswith('.las'):
        #             ingestor.ingest_las_file(os.path.join(las_dir, f))