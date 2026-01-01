import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from pyproj import CRS, Transformer, Proj

class TrajectoryProcessor:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def _minimum_curvature(self, md, inc, azi):
        # ... (Same min curve math as previous, ensure it's here) ...
        # (For brevity in the "Day 1" answer, assuming standard MCM implementation)
        inc_rad = np.radians(inc)
        azi_rad = np.radians(azi)
        d_md = np.diff(md, prepend=md[0])
        i1, i2 = inc_rad[:-1], inc_rad[1:]
        a1, a2 = azi_rad[:-1], azi_rad[1:]
        cos_beta = np.clip(np.cos(i1)*np.cos(i2) + np.sin(i1)*np.sin(i2)*np.cos(a2-a1), -1, 1)
        beta = np.arccos(cos_beta)
        rf = np.zeros_like(beta)
        mask = beta > 0.0001
        rf[mask] = (2 / beta[mask]) * np.tan(beta[mask] / 2)
        rf[~mask] = 1.0 
        dm = d_md[1:]
        dy = (dm/2) * (np.sin(i1)*np.cos(a1) + np.sin(i2)*np.cos(a2)) * rf
        dx = (dm/2) * (np.sin(i1)*np.sin(a1) + np.sin(i2)*np.sin(a2)) * rf
        dtvd = (dm/2) * (np.cos(i1) + np.cos(i2)) * rf
        return np.concatenate(([0], np.cumsum(dtvd))), np.concatenate(([0], np.cumsum(dx))), np.concatenate(([0], np.cumsum(dy)))

    def process_well(self, wellbore_id: str):
        with self.engine.connect() as conn:
            # 1. Fetch Data
            row = conn.execute(text("""
                SELECT ds.survey_points, ds.azimuth_ref, ST_X(w.surface_geom) as lon, ST_Y(w.surface_geom) as lat
                FROM directional_surveys ds
                JOIN wellbore_master wb ON ds.wellbore_id = wb.wellbore_id
                JOIN well_master w ON wb.well_id = w.well_id
                WHERE ds.wellbore_id = :wb_id AND ds.is_active = TRUE
            """), {"wb_id": wellbore_id}).fetchone()
            
            if not row or not row.survey_points: return

            # 2. Determine CRS & Convergence
            zone = int((row.lon + 180) / 6) + 1
            epsg = 26900 + zone # NAD83 UTM
            convergence = Proj(f"EPSG:{epsg}").get_factors(row.lon, row.lat).meridian_convergence

            # 3. Rotate Azimuth
            df = pd.DataFrame(row.survey_points).sort_values('md')
            grid_azi = df['azi'].values if str(row.azimuth_ref).lower() == 'grid north' else df['azi'].values - convergence
            
            # 4. Compute
            tvd, east, north = self._minimum_curvature(df['md'].values, df['inc'].values, grid_azi)
            
            # 5. Project & Store
            trans = Transformer.from_crs("EPSG:4269", f"EPSG:{epsg}", always_xy=True)
            sx, sy = trans.transform(row.lon, row.lat)
            
            # Convert Offsets (Feet) to Meters for UTM addition
            abs_x, abs_y = sx + (east * 0.3048), sy + (north * 0.3048)
            
            # Unproject to Lat/Lon for storage
            to_geo = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4269", always_xy=True)
            flon, flat = to_geo.transform(abs_x, abs_y)
            
            wkt = f"LINESTRING Z({', '.join([f'{x:.7f} {y:.7f} {z:.2f}' for x,y,z in zip(flon, flat, tvd)])})"
            
            conn.execute(text("""
                UPDATE wellbore_master SET trajectory_geom = ST_GeomFromText(:wkt, 4269),
                total_depth_md = :md, total_depth_tvd = :tvd, crs_epsg = :epsg, grid_convergence = :conv
                WHERE wellbore_id = :wb
            """), {"wkt": wkt, "md": float(df['md'].max()), "tvd": float(tvd[-1]), "epsg": epsg, "conv": convergence, "wb": wellbore_id})
            print(f"✅ Updated {wellbore_id} (Conv: {convergence:.2f})")