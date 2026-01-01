import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from pyproj import Proj, Transformer

class TrajectoryProcessor:
    def __init__(self, db_url):
        if not db_url:
            raise ValueError("DB_URL is not set.")
        self.engine = create_engine(db_url)

    def _minimum_curvature(self, md, inc, azi, start_elev=0):
        """
        Calculates 3D points using Minimum Curvature Method.
        Returns: 
          - TVD (Vertical depth from KB, Positive Down)
          - North (Offset from Surface)
          - East (Offset from Surface)
          - True_Z (Elevation from Sea Level, Positive Up/Negative Down)
        """
        # Convert to Radians
        inc_rad = np.radians(inc)
        azi_rad = np.radians(azi)
        
        # Intervals
        d_md = np.diff(md, prepend=md[0])
        
        # Angles
        i1, i2 = inc_rad[:-1], inc_rad[1:]
        a1, a2 = azi_rad[:-1], azi_rad[1:]
        
        # Dogleg Severity (Beta)
        cos_beta = np.clip(np.cos(i1)*np.cos(i2) + np.sin(i1)*np.sin(i2)*np.cos(a2-a1), -1, 1)
        beta = np.arccos(cos_beta)
        
        # Ratio Factor (Handle straight holes)
        rf = np.zeros_like(beta)
        mask = beta > 0.0001
        rf[mask] = (2 / beta[mask]) * np.tan(beta[mask] / 2)
        rf[~mask] = 1.0 
        
        # Deltas
        dm = d_md[1:] # First diff is 0 anyway for prepend
        # Note: d_md calculation above prepended md[0], so d_md is same length as md. 
        # But i1/i2 are length-1. We need consistent lengths.
        # FIX:
        d_md = np.diff(md)
        d_md = np.insert(d_md, 0, 0) # Start at 0
        
        # Re-slice for the loop (skip first point which is surface)
        # Actually standard MCM vectorization:
        # We need d_md between points i and i+1.
        d_sec = np.diff(md)
        i1, i2 = inc_rad[:-1], inc_rad[1:]
        a1, a2 = azi_rad[:-1], azi_rad[1:]
        
        # Recalc Beta/RF with correct shapes
        cos_beta = np.clip(np.cos(i1)*np.cos(i2) + np.sin(i1)*np.sin(i2)*np.cos(a2-a1), -1, 1)
        beta = np.arccos(cos_beta)
        rf = np.zeros_like(beta)
        mask = beta > 0.0001
        rf[mask] = (2 / beta[mask]) * np.tan(beta[mask] / 2)
        rf[~mask] = 1.0 

        dn = (d_sec/2) * (np.sin(i1)*np.cos(a1) + np.sin(i2)*np.cos(a2)) * rf
        de = (d_sec/2) * (np.sin(i1)*np.sin(a1) + np.sin(i2)*np.sin(a2)) * rf
        dtvd = (d_sec/2) * (np.cos(i1) + np.cos(i2)) * rf
        
        # Integrate
        tvd = np.concatenate(([0], np.cumsum(dtvd)))
        north = np.concatenate(([0], np.cumsum(dn)))
        east = np.concatenate(([0], np.cumsum(de)))
        
        # GIS Z-Axis: True Elevation = KB Elevation - TVD
        # Example: KB=100, TVD=5000 -> Z = -4900
        true_z = start_elev - tvd 
        
        return tvd, north, east, true_z

    def calculate_well(self, wellbore_id: str):
        """Calculates trajectory, updates Geometry, and syncs Tops TVD."""
        with self.engine.connect() as conn:
            # 1. Fetch Data
            # Note column name updates: lat_surface, lon_surface
            row = conn.execute(text("""
                SELECT ds.survey_points, ds.azimuth_ref, 
                       w.lat_surface, w.lon_surface, w.elevation_kb
                FROM directional_surveys ds
                JOIN wellbore_master wb ON ds.wellbore_id = wb.wellbore_id
                JOIN well_master w ON wb.well_id = w.well_id
                WHERE ds.wellbore_id = :wb AND ds.is_active = TRUE
            """), {"wb": wellbore_id}).fetchone()
            
            if not row or not row.survey_points: return

            kb = float(row.elevation_kb) if row.elevation_kb else 0.0
            lat = float(row.lat_surface)
            lon = float(row.lon_surface)
            
            # 2. Setup Data
            df = pd.DataFrame(row.survey_points).sort_values('md')
            if df.empty: return

            # 3. Determine CRS & Convergence
            # Dynamic Zone Calculation
            zone = int((lon + 180) / 6) + 1
            epsg = 26900 + zone # NAD83 UTM Zone X
            
            # Calculate Grid Convergence
            p = Proj(f"EPSG:{epsg}")
            factors = p.get_factors(lon, lat)
            convergence = factors.meridian_convergence
            
            # Apply Convergence if North Ref is True North
            grid_azi = df['azi'].values
            if str(row.azimuth_ref).lower() == 'true north':
                grid_azi = df['azi'].values - convergence
            
            # 4. Run MCM Calculation
            tvd_arr, n_arr, e_arr, z_arr = self._minimum_curvature(
                df['md'].values, df['inc'].values, grid_azi, start_elev=kb
            )
            
            # 5. Project to Absolute Coordinates for WKT
            # Transform Lat/Lon (4269) to UTM (EPSG) to add offsets
            to_utm = Transformer.from_crs("EPSG:4269", f"EPSG:{epsg}", always_xy=True)
            to_geo = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4269", always_xy=True)
            
            sx, sy = to_utm.transform(lon, lat)
            
            # Add offsets (convert feet to meters for UTM addition)
            # Assumption: survey offsets are Feet. 1 ft = 0.3048 m
            abs_x = sx + (e_arr * 0.3048)
            abs_y = sy + (n_arr * 0.3048)
            
            # Back to Lat/Lon for storage
            flon, flat = to_geo.transform(abs_x, abs_y)
            
            # 6. Store Result
            # WKT format: LINESTRING Z (lon lat z, lon lat z)
            wkt_points = [f"{x:.7f} {y:.7f} {z:.2f}" for x,y,z in zip(flon, flat, z_arr)]
            wkt = f"LINESTRING Z({', '.join(wkt_points)})"
            
            conn.execute(text("""
                UPDATE wellbore_master SET 
                    trajectory_geom = ST_GeomFromText(:wkt, 4269),
                    total_depth_tvd = :tvd,
                    crs_epsg = :epsg,
                    grid_convergence = :conv
                WHERE wellbore_id = :wb
            """), {
                "wkt": wkt, "tvd": float(tvd_arr[-1]), 
                "epsg": epsg, "conv": convergence, "wb": wellbore_id
            })
            
            print(f"✅ Trajectory Updated: {wellbore_id} (TVD: {tvd_arr[-1]:.1f})")
            
            # 7. Sync Tops (Physics Integration)
            self._recalc_tops_physics(conn, wellbore_id, df['md'].values, tvd_arr)

    def _recalc_tops_physics(self, conn, wb_id, survey_md, survey_tvd):
        """Linearly interpolates TVD for tops based on the new survey."""
        
        # Check Dependency
        if not inspect(self.engine).has_table('formation_tops'):
            return 

        # Get Tops
        tops = conn.execute(text("SELECT top_id, depth_md FROM formation_tops WHERE wellbore_id = :wb"), 
                          {"wb": wb_id}).fetchall()
        
        if not tops: return

        update_data = []
        for t in tops:
            # Interpolate TVD at Top MD
            if t.depth_md > survey_md.max():
                pass # Top is deeper than survey; cannot calculate
            elif t.depth_md < survey_md.min():
                pass # Top is shallower than survey start
            else:
                calc_tvd = np.interp(t.depth_md, survey_md, survey_tvd)
                update_data.append({"tid": t.top_id, "tvd": float(calc_tvd)})
        
        if update_data:
            conn.execute(text("UPDATE formation_tops SET depth_tvd = :tvd WHERE top_id = :tid"), update_data)
            print(f"   -> Synced TVD for {len(update_data)} tops.")

if __name__ == "__main__":
    # Example usage
    db_url = "postgresql+psycopg2://postgres:password@localhost/subsurface"
    # tp = TrajectoryProcessor(db_url)
    # tp.calculate_well("some-uuid")