import pandas as pd
import json
import os
import lasio
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load env variables
load_dotenv()

DB_CONNECTION_STR = os.getenv("DB_URL")
CONFIG_FILE = "field_mapping.json"
LAKE_STORE_PATH = "./lake_data_parquet"

class SubsurfaceIngestor:
    def __init__(self, db_url: str, config_file: str, capture_unknowns: bool = True):
        self.engine: Engine = create_engine(db_url)
        self.capture_unknowns = capture_unknowns
        with open(config_file, 'r') as f:
            self.mappings: dict[str, dict[str, list[str]]] = json.load(f)

    def _process_dataframe(self, df: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
        """Normalizes mapped columns and optionally captures unmapped ones."""
        target_map = self.mappings[mapping_key]
        rename_map = {}
        mapped_source_cols = set()

        for target_col, aliases in target_map.items():
            upper_aliases = {a.upper() for a in aliases}
            for csv_col in df.columns:
                if csv_col.upper() in upper_aliases:
                    rename_map[csv_col] = target_col
                    mapped_source_cols.add(csv_col)
                    break 
        
        df_clean = df.rename(columns=rename_map)
        
        if self.capture_unknowns:
            unknown_cols = [c for c in df.columns if c not in mapped_source_cols]
            if unknown_cols:
                # Vectorized JSON creation (Much faster than row iteration)
                print(f"   -> Capturing {len(unknown_cols)} unknown columns.")
                df_clean['attributes'] = df[unknown_cols].to_dict(orient='records')
            else:
                df_clean['attributes'] = [{} for _ in range(len(df))]
        else:
            df_clean['attributes'] = [{} for _ in range(len(df))]

        valid_cols = list(target_map.keys()) + ['attributes']
        existing_cols = [c for c in valid_cols if c in df_clean.columns]
        return df_clean[existing_cols]

    def _get_id_map(self, conn, table, key_col, id_col, keys):
        """
        Efficiently fetches a dictionary of {key: id} for a list of keys.
        """
        if not keys: return {}
        
        # Format keys for SQL IN clause safe parameter binding
        # (Using a simplified approach for bulk fetch)
        query = text(f"SELECT {key_col}, {id_col} FROM {table} WHERE {key_col} IN :keys")
        result = conn.execute(query, {"keys": tuple(keys)}).fetchall()
        return {row[0]: row[1] for row in result}

    def ingest_headers_csv(self, csv_path: str) -> None:
        print(f"🔹 Processing Header File: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)
        df = self._process_dataframe(df, "well_header_mappings")
        
        # 1. Vectorized Cleanup
        # Remove non-alphanumeric from UWI
        df['uwi'] = df['uwi'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        
        # Ensure lat/lon are numeric, coerce errors to NaN
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        
        # Drop rows with no UWI (Critical)
        df = df.dropna(subset=['uwi'])

        # Prepare list of dicts for bulk insert
        records = df.to_dict(orient='records')
        
        # 2. Bulk Insert / Upsert Wells
        # We handle the JSON logic in SQL so we don't need to loop in Python
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

            # 3. Handle Wellbores (Bulk)
            # Fetch all Well IDs for the UWIs we just touched
            uwi_list = df['uwi'].unique().tolist()
            well_map = self._get_id_map(conn, 'well_master', 'uwi', 'well_id', uwi_list)
            
            # Prepare wellbore records
            wb_records = []
            for uwi in uwi_list:
                if uwi in well_map:
                    wb_records.append({"well_id": well_map[uwi], "wb_name": "OH"})
            
            if wb_records:
                print(f"   -> Ensuring default wellbores for {len(wb_records)} wells...")
                conn.execute(text("""
                    INSERT INTO wellbore_master (well_id, wellbore_name)
                    VALUES (:well_id, :wb_name)
                    ON CONFLICT DO NOTHING
                """), wb_records)
                
        print("✅ Headers Loaded via Bulk Insert.")

    def ingest_tops_csv(self, csv_path: str) -> None:
        print(f"🔹 Processing Tops File: {csv_path}")
        df = pd.read_csv(csv_path)
        df = self._process_dataframe(df, "tops_mappings")
        
        # Cleanup
        df['uwi'] = df['uwi'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        df['formation'] = df['formation'].str.strip()
        df = df.dropna(subset=['uwi', 'formation', 'depth'])
        
        with self.engine.begin() as conn:
            # 1. Resolve Strat Units (Formations)
            unique_fms = df['formation'].unique().tolist()
            
            # Insert new formations if they don't exist
            conn.execute(text("""
                INSERT INTO strat_unit_dictionary (unit_name)
                VALUES (:unit_name)
                ON CONFLICT (unit_name) DO NOTHING
            """), [{"unit_name": fm} for fm in unique_fms])
            
            # Fetch Map: {'Eagle Ford': 101, 'Austin Chalk': 102}
            strat_map = self._get_id_map(conn, 'strat_unit_dictionary', 'unit_name', 'strat_unit_id', unique_fms)
            
            # 2. Resolve Wellbores
            # We need to link UWI -> Well -> Wellbore
            # (Assuming primary wellbore 'OH' for simplicity, or could be expanded)
            unique_uwis = df['uwi'].unique().tolist()
            
            # Join query to get UWI -> Wellbore_ID directly
            wb_query = text("""
                SELECT w.uwi, wb.wellbore_id 
                FROM wellbore_master wb 
                JOIN well_master w ON wb.well_id = w.well_id 
                WHERE w.uwi IN :uwis
            """)
            wb_res = conn.execute(wb_query, {"uwis": tuple(unique_uwis)}).fetchall()
            wb_map = {row[0]: row[1] for row in wb_res}
            
            # 3. Map IDs to DataFrame
            # (Vectorized mapping is faster than row iteration)
            df['strat_unit_id'] = df['formation'].map(strat_map)
            df['wellbore_id'] = df['uwi'].map(wb_map)
            
            # Drop rows where we couldn't find the well or the formation
            missing_wells = df[df['wellbore_id'].isna()]
            if not missing_wells.empty:
                print(f"   ⚠️ Skipping {len(missing_wells)} tops (Wells not found in DB)")
            
            valid_tops = df.dropna(subset=['wellbore_id', 'strat_unit_id']).copy()
            
            # 4. Bulk Insert Tops
            if not valid_tops.empty:
                tops_records = valid_tops[[
                    'wellbore_id', 'strat_unit_id', 'depth', 
                    'interpreter', 'quality'
                ]].to_dict(orient='records')
                
                # Rename columns to match SQL bind params if needed, or alias in SQL
                # Here we map DF column names to bind params
                print(f"   -> Bulk inserting {len(tops_records)} tops...")
                conn.execute(text("""
                    INSERT INTO formation_tops (wellbore_id, strat_unit_id, depth_md, interpreter, pick_quality)
                    VALUES (:wellbore_id, :strat_unit_id, :depth, :interpreter, :quality)
                """), tops_records)

        print("✅ Tops Loaded via Bulk Insert.")

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