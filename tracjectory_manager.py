import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine, text
from pyproj import CRS, Transformer
from shapely.geometry import LineString
from shapely.ops import transform

class TrajectoryProcessor:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def _minimum_curvature(self, md, inc, azi):
        """
        Vectorized Minimum Curvature Algorithm.
        Inputs: Arrays of MD (ft), Inc (deg), Azi (deg).
        Returns: Arrays of TVD, DX (East), DY (North).
        """
        # Convert to radians
        inc_rad = np.radians(inc)
        azi_rad = np.radians(azi)
        
        # Calculate intervals (delta)
        d_md = np.diff(md, prepend=md[0])
        
        # Average angles for ratio calculation
        # Note: This is a simplified implementation. 
        # Robust implementations handle "dogleg severity" checks here.
        
        # We need pairs of points (i-1, i)
        i1 = inc_rad[:-1]
        i2 = inc_rad[1:]
        a1 = azi_rad[:-1]
        a2 = azi_rad[1:]
        
        # Dogleg Angle (Beta)
        # cos(beta) = cos(I1)cos(I2) + sin(I1)sin(I2)cos(A2-A1)
        cos_beta = np.cos(i1)*np.cos(i2) + np.sin(i1)*np.sin(i2)*np.cos(a2-a1)
        
        # Clip to avoid numeric errors (acos domain -1 to 1)
        cos_beta = np.clip(cos_beta, -1.0, 1.0)
        beta = np.arccos(cos_beta)
        
        # Ratio Factor (RF) = 2/beta * tan(beta/2)
        # Handle zero beta (straight sections) to avoid division by zero
        rf = np.zeros_like(beta)
        mask = beta > 0.0001
        rf[mask] = (2 / beta[mask]) * np.tan(beta[mask] / 2)
        rf[~mask] = 1.0 # Linear approximation for very small curves
        
        # Deltas
        # North = dMD/2 * (sinI1*cosA1 + sinI2*cosA2) * RF
        # East  = dMD/2 * (sinI1*sinA1 + sinI2*sinA2) * RF
        # TVD   = dMD/2 * (cosI1 + cosI2) * RF
        
        dm = d_md[1:] # Skip first point (0)
        
        dy = (dm/2) * (np.sin(i1)*np.cos(a1) + np.sin(i2)*np.cos(a2)) * rf
        dx = (dm/2) * (np.sin(i1)*np.sin(a1) + np.sin(i2)*np.sin(a2)) * rf
        dtvd = (dm/2) * (np.cos(i1) + np.cos(i2)) * rf
        
        # Accumulate
        # Start at 0,0,0
        north = np.concatenate(([0], np.cumsum(dy)))
        east = np.concatenate(([0], np.cumsum(dx)))
        tvd = np.concatenate(([0], np.cumsum(dtvd)))
        
        return tvd, east, north

    def _get_utm_transformer(self, lon, lat):
        """
        Dynamically finds the UTM CRS for a given Long/Lat
        and returns a transformer to Project (LatLon -> Meters) and UnProject.
        """
        # EPSG 4269 is NAD83 Lat/Lon
        crs_src = CRS.from_epsg(4269) 
        
        # Simple UTM Zone calculation
        zone = int((lon + 180) / 6) + 1
        hemisphere = 'north' if lat >= 0 else 'south'
        
        # Construct UTM CRS string (e.g., "+proj=utm +zone=14 +datum=NAD83 ...")
        # For simplicity in US, we often default to EPSG codes, but dynamic proj string is safer globally
        crs_utm = CRS.from_dict({
            'proj': 'utm', 'zone': zone, 'south': hemisphere == 'south', 'ellps': 'GRS80'
        })
        
        # Transformer: LatLon -> UTM
        to_utm = Transformer.from_crs(crs_src, crs_utm, always_xy=True)
        # Transformer: UTM -> LatLon
        to_geo = Transformer.from_crs(crs_utm, crs_src, always_xy=True)
        
        return to_utm, to_geo

    def process_well(self, wellbore_id: str):
        """
        Reads raw JSON survey, computes geometry, updates DB.
        """
        with self.engine.connect() as conn:
            # 1. Fetch Data
            # We join wellbore -> well to get the Surface Location
            sql = text("""
                SELECT 
                    ds.survey_points, 
                    ST_X(w.surface_geom) as lon, 
                    ST_Y(w.surface_geom) as lat
                FROM directional_surveys ds
                JOIN wellbore_master wb ON ds.wellbore_id = wb.wellbore_id
                JOIN well_master w ON wb.well_id = w.well_id
                WHERE ds.wellbore_id = :wb_id AND ds.is_active = TRUE
            """)
            row = conn.execute(sql, {"wb_id": wellbore_id}).fetchone()
            
            if not row:
                print(f"No active survey found for {wellbore_id}")
                return

            points_data = row.survey_points # This is the JSON list
            surf_lon, surf_lat = row.lon, row.lat
            
            if not points_data:
                return

            # 2. Parse JSON to Numpy
            df = pd.DataFrame(points_data)
            # Ensure columns exist and sort by depth
            if not {'md', 'inc', 'azi'}.issubset(df.columns):
                print("Survey JSON missing required columns (md, inc, azi)")
                return
            
            df = df.sort_values('md')
            md = df['md'].values
            inc = df['inc'].values
            azi = df['azi'].values
            
            # 3. Compute 3D Offsets (TVD, North, East) in FEET/METERS
            # (Assuming input MD is Feet, output offsets are Feet)
            tvd_arr, east_arr, north_arr = self._minimum_curvature(md, inc, azi)
            
            # 4. Project Surface to Meters (UTM)
            to_utm, to_geo = self._get_utm_transformer(surf_lon, surf_lat)
            surf_x, surf_y = to_utm.transform(surf_lon, surf_lat)
            
            # 5. Apply Offsets
            # (Requires unit check: If UTM is meters but Survey is Feet, convert offsets)
            # ASSUMPTION: Survey is US Feet, UTM is Meters.
            FEET_TO_METERS = 0.3048
            abs_x = surf_x + (east_arr * FEET_TO_METERS)
            abs_y = surf_y + (north_arr * FEET_TO_METERS)
            
            # 6. Un-Project back to Lat/Lon
            # Vectorized transform is faster
            final_lon, final_lat = to_geo.transform(abs_x, abs_y)
            
            # 7. Construct WKT LineString Z
            # Format: LINESTRING Z (lon lat depth, lon lat depth, ...)
            # Note: PostGIS Z is usually "Height", so Depth might need to be negative 
            # or just treated as positive depth. Let's use negative for "Subsea" visualization 
            # or positive for "Depth". Subsurface tools usually prefer Negative Z = Down.
            # But standard OSDU/ResqML often keeps Z positive downwards. 
            # Let's keep Z positive (Depth) to match the MD/TVD columns.
            
            coords = list(zip(final_lon, final_lat, tvd_arr))
            wkt_parts = [f"{x:.7f} {y:.7f} {z:.2f}" for x, y, z in coords]
            wkt = f"LINESTRING Z({', '.join(wkt_parts)})"
            
            # 8. Update DB
            # We also update the total depths while we are here
            max_md = float(md[-1])
            max_tvd = float(tvd_arr[-1])
            
            update_sql = text("""
                UPDATE wellbore_master
                SET trajectory_geom = ST_GeomFromText(:wkt, 4269),
                    total_depth_md = :max_md,
                    total_depth_tvd = :max_tvd,
                    updated_at = CURRENT_TIMESTAMP
                WHERE wellbore_id = :wb_id
            """)
            
            conn.execute(update_sql, {
                "wkt": wkt, 
                "max_md": max_md, 
                "max_tvd": max_tvd, 
                "wb_id": wellbore_id
            })
            print(f"✅ Trajectory updated for {wellbore_id}")

# Example Usage
# processor = TrajectoryProcessor(os.getenv("DB_URL"))
# processor.process_well("uuid-of-wellbore")