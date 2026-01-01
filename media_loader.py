import os
import re
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

class MediaLoader:
    def __init__(self, db_url: str):
        if not db_url:
            raise ValueError("DB_URL is not set.")
        self.engine = create_engine(db_url)
        
        # Regex for finding depths in filenames (e.g., "3500-3510", "3500ft")
        self.regex_range = re.compile(r'[-_ ](\d{1,5}(?:\.\d+)?)[-_ ](?:to|[-_])[-_ ]?(\d{1,5}(?:\.\d+)?)(?=[^\d]|$)', re.IGNORECASE)
        self.regex_single = re.compile(r'[-_ ](\d{1,5}(?:\.\d+)?)(?:ft|m|md)?(?=[-_ .]|$)', re.IGNORECASE)

    def _preload_well_cache(self, conn):
        """Fetches all UWIs to create a clean matching dictionary."""
        # Strips non-numerics from DB UWI: '42-123-456' -> '42123456'
        rows = conn.execute(text("""
            SELECT regexp_replace(w.uwi, '[^0-9]', '', 'g'), wb.wellbore_id
            FROM wellbore_master wb JOIN well_master w ON wb.well_id = w.well_id
        """)).fetchall()
        return {row[0]: row[1] for row in rows}

    def _infer_context(self, filename: str):
        fname = filename.lower()
        if 'core' in fname and 'photo' in fname:
            return 'Core Photo', 'White Light' if 'uv' not in fname else 'UV Light'
        if 'thin_section' in fname:
            return 'Thin Section', 'Micrograph'
        if 'log' in fname and '.tif' in fname:
            return 'Raster Log', 'Scanned Image'
        if 'report' in fname or '.pdf' in fname:
            return 'Document', 'Report'
        return 'General Media', 'Auto-Import'

    def scan_directory(self, root_path: str):
        if not inspect(self.engine).has_table('media_catalog'):
            print("❌ Error: 'media_catalog' table missing. Install Geology module.")
            return

        print(f"🚀 Scanning {root_path}")
        root = Path(root_path)
        if not root.exists():
            print("❌ Path does not exist.")
            return

        with self.engine.begin() as conn:
            cache = self._preload_well_cache(conn)
            count = 0

            for file_path in root.rglob('*'):
                if not file_path.is_file() or file_path.name.startswith('.'):
                    continue

                # Clean filename to find UWI candidates
                clean_name = re.sub(r'[^0-9]', '', file_path.name)
                # Find all sequences of 10-14 digits
                candidates = re.findall(r'\d{10,14}', clean_name)
                
                matched_wb = None
                for c in candidates:
                    if c in cache:
                        matched_wb = cache[c]
                        break
                
                if matched_wb:
                    # Check duplicate
                    exists = conn.execute(text("SELECT 1 FROM media_catalog WHERE file_path = :p"), 
                                        {"p": str(file_path.absolute())}).scalar()
                    if exists: continue

                    # Meta
                    mtype, desc = self._infer_context(file_path.name)
                    
                    # Depths
                    top, base = None, None
                    m_range = self.regex_range.search(file_path.name)
                    if m_range:
                        d1, d2 = float(m_range.group(1)), float(m_range.group(2))
                        top, base = min(d1, d2), max(d1, d2)
                    else:
                        m_single = self.regex_single.search(file_path.name)
                        if m_single:
                            top = base = float(m_single.group(1))

                    conn.execute(text("""
                        INSERT INTO media_catalog (wellbore_id, media_type, file_format, file_path, top_depth_md, base_depth_md, description)
                        VALUES (:wb, :mt, :fmt, :fp, :top, :base, :desc)
                    """), {
                        "wb": matched_wb, "mt": mtype, "fmt": file_path.suffix.lstrip('.'),
                        "fp": str(file_path.absolute()), "top": top, "base": base,
                        "desc": f"{desc} | Source: {file_path.parent.name}"
                    })
                    count += 1
            
            print(f"✅ Indexed {count} new files.")

if __name__ == "__main__":
    db_url = os.getenv("DB_URL")
    if db_url:
        loader = MediaLoader(db_url)
        # loader.scan_directory("/path/to/files")