import os
import re
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Load environment variables if .env exists
load_dotenv()

class MediaLoader:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        
        # --- REGEX PATTERNS ---
        
        # 1. Depth Ranges (e.g., "3500-3510", "3500_to_3510")
        # Captures Group 1 (Top) and Group 2 (Base)
        self.regex_depth_range = re.compile(r'[-_ ](\d{1,5}(?:\.\d+)?)[-_ ](?:to|[-_])[-_ ]?(\d{1,5}(?:\.\d+)?)(?=[^\d]|$)', re.IGNORECASE)
        
        # 2. Single Depth (e.g., "3500ft", "_3500_")
        # Captures Group 1 (Depth)
        self.regex_single_depth = re.compile(r'[-_ ](\d{1,5}(?:\.\d+)?)(?:ft|m|md)?(?=[-_ .]|$)', re.IGNORECASE)

    def _preload_well_cache(self, conn):
        """
        Fetches all known UWIs from the database to create a fast lookup map.
        Returns: { '42123456780000': 'uuid-of-wellbore', ... }
        """
        print("   -> Pre-loading well inventory...")
        # We strip non-numeric characters from the DB UWI to ensure matching works
        # regardless of how it's stored (dashed or not).
        # We prioritize the 'OH' (Open Hole) wellbore, but you could adjust this logic.
        sql = text("""
            SELECT 
                regexp_replace(w.uwi, '[^0-9]', '', 'g') as clean_uwi,
                wb.wellbore_id
            FROM wellbore_master wb
            JOIN well_master w ON wb.well_id = w.well_id
            WHERE wb.wellbore_name = 'OH' 
        """)
        
        rows = conn.execute(sql).fetchall()
        cache = {row[0]: row[1] for row in rows}
        print(f"   -> Cached {len(cache)} wells for rapid matching.")
        return cache

    def _infer_media_context(self, filename: str):
        """
        Analyzes filename to determine Category, Type, and Light Source.
        Returns: (media_type, description_tag)
        """
        fname = filename.lower()
        
        # --- 1. CORE PHOTOS ---
        if any(x in fname for x in ['core_photo', 'slab', 'box', 'tray']):
            if 'uv' in fname or 'ultraviolet' in fname:
                return 'Core Photo', 'UV Light'
            return 'Core Photo', 'White Light'
            
        # --- 2. THIN SECTIONS ---
        if any(x in fname for x in ['thin_section', 'ts_', 'micrograph']):
            if 'xpl' in fname or 'cross_polar' in fname:
                return 'Thin Section Photo', 'Cross-Polarized (XPL)'
            if 'ppl' in fname or 'plane_polar' in fname:
                return 'Thin Section Photo', 'Plane-Polarized (PPL)'
            return 'Thin Section Photo', 'Unknown Light'

        # --- 3. SEM (Scanning Electron Microscope) ---
        if 'sem' in fname or 'scanning_electron' in fname:
            return 'SEM Image', 'Microscopy'

        # --- 4. DOCUMENTS & REPORTS ---
        if fname.endswith('pdf') or fname.endswith('doc') or fname.endswith('docx'):
            if 'mudlog' in fname:
                return 'Mudlog', 'Geological Report'
            if 'core_desc' in fname or 'description' in fname:
                return 'Core Description', 'Digitized Log'
            if 'routine' in fname or 'rca' in fname:
                return 'Lab Report', 'Routine Core Analysis'
            if 'special' in fname or 'scal' in fname:
                return 'Lab Report', 'Special Core Analysis'
            return 'Document', 'General Report'

        # --- 5. RASTER LOGS ---
        if any(x in fname for x in ['.tif', '.tiff']) and ('log' in fname or 'composite' in fname):
            return 'Raster Log', 'Scanned Log Image'

        # Default
        return 'General Media', 'Auto-Imported'

    def _extract_depths(self, filename: str):
        """
        Tries to extract Top and Base depth from filename.
        Returns: (top, base) or (None, None)
        """
        # Try Range First: "Box1_4500-4510.jpg"
        match_range = self.regex_depth_range.search(filename)
        if match_range:
            try:
                d1, d2 = float(match_range.group(1)), float(match_range.group(2))
                return min(d1, d2), max(d1, d2)
            except ValueError:
                pass

        # Try Single Depth: "Plug_4505.jpg"
        match_single = self.regex_single_depth.search(filename)
        if match_single:
            try:
                d = float(match_single.group(1))
                return d, d # Top == Base for a specific point
            except ValueError:
                pass
                
        return None, None

    def scan_directory(self, root_path: str):
        """
        Recursively scans a directory, matches files to cached Wells, 
        extracts metadata, and loads into DB.
        """
        print(f"🚀 Starting Media Scan: {root_path}")
        root = Path(root_path)
        
        if not root.exists():
            print(f"❌ Path not found: {root_path}")
            return

        new_files = 0
        skipped_files = 0
        errors = 0

        with self.engine.begin() as conn:
            # 1. Build the Cache (The robust fix)
            uwi_cache = self._preload_well_cache(conn)

            for file_path in root.rglob('*'):
                if not file_path.is_file():
                    continue
                
                # Exclude hidden files
                if file_path.name.startswith('.'):
                    continue

                # 2. MATCH UWI (The Robust "Clean & Check" Method)
                # Strip everything except numbers from the filename
                # Example: "Eagle_Ford-42-123-45678_Box1.jpg" -> "42123456781"
                clean_name = re.sub(r'[^0-9]', '', file_path.name)
                
                # Also check parent folder name if filename is generic (e.g. "photo1.jpg")
                clean_parent = re.sub(r'[^0-9]', '', file_path.parent.name)
                
                # Find candidate UWIs (10 to 14 digits) inside the cleaned strings
                # We use a set to avoid duplicates
                candidates = set(re.findall(r'\d{10,14}', clean_name))
                candidates.update(re.findall(r'\d{10,14}', clean_parent))
                
                matched_wb_id = None
                
                # Check candidates against our DB cache
                for cand in candidates:
                    if cand in uwi_cache:
                        matched_wb_id = uwi_cache[cand]
                        break # Found a match, stop looking
                
                if not matched_wb_id:
                    skipped_files += 1
                    continue # No known well found in filename

                # 3. CHECK DUPLICATES
                full_path_str = str(file_path.absolute())
                exists = conn.execute(text(
                    "SELECT 1 FROM media_catalog WHERE file_path = :p"
                ), {"p": full_path_str}).scalar()
                
                if exists:
                    continue

                # 4. EXTRACT METADATA
                media_type, desc_tag = self._infer_media_context(file_path.name)
                top, base = self._extract_depths(file_path.name)
                
                # 5. INSERT
                try:
                    conn.execute(text("""
                        INSERT INTO media_catalog 
                        (wellbore_id, media_type, file_format, file_path, 
                         top_depth_md, base_depth_md, description)
                        VALUES 
                        (:wb, :type, :fmt, :path, :top, :base, :desc)
                    """), {
                        "wb": matched_wb_id,
                        "type": media_type,
                        "fmt": file_path.suffix.lstrip('.').lower(),
                        "path": full_path_str,
                        "top": top,
                        "base": base,
                        "desc": f"{desc_tag} | Source: {file_path.parent.name}"
                    })
                    new_files += 1
                    if new_files % 100 == 0:
                        print(f"   ...cataloged {new_files} files")
                        
                except Exception as e:
                    print(f"Error inserting {file_path.name}: {e}")
                    errors += 1

        print(f"✅ Scan Complete.")
        print(f"   - New Files Linked: {new_files}")
        print(f"   - Skipped (No Match): {skipped_files}")
        print(f"   - Errors: {errors}")

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    # Ensure you have your DB_URL set in .env or hardcoded here for testing
    # DB_URL = "postgresql+psycopg2://postgres:password@localhost/subsurface_db"
    
    db_url = os.getenv("DB_URL")
    if not db_url:
        print("❌ DB_URL not found in environment.")
    else:
        # Initialize Loader
        loader = MediaLoader(db_url)
        
        # Point this at your raw data dump
        # loader.scan_directory("/path/to/your/data/drive")