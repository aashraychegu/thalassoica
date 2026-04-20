"""
db.py

Data access layer for the overlaps database.
Geometry filtering is done in Python from GCPs read off the TIFFs,
since the geometry BLOBs in DuckDB are in a format we cannot parse.
"""

import duckdb
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from shapely.geometry import box

DB_PATH = "database.duckdb" #REPLACE WITH YOUR PATH
TABLE = "sentinel1__product_filtered_matches_overlaps__overlap_filtered__era5_filtered"
TIFF_DIR = Path("./pairs") #REPLACE WITH YOUR PATH


@dataclass
class PairRecord:
    id_before: str
    id_after: str

    # Extend with ERA5 fields as needed:
    # wind_speed: Optional[float] = None
    # precipitation: Optional[float] = None

    @property
    def label(self):
        return self.id_before[:8] + "... / " + self.id_after[:8] + "..."

    @property
    def folder_before(self) -> Path:
        return TIFF_DIR / self.id_before

    @property
    def folder_after(self) -> Path:
        return TIFF_DIR / self.id_after

    @property
    def available_locally(self) -> bool:
        return self.folder_before.is_dir() and self.folder_after.is_dir()

    def overlap_intersects(self,
                           lon_min: Optional[float], lon_max: Optional[float],
                           lat_min: Optional[float], lat_max: Optional[float]) -> bool:
        """
        Return True if the overlap of the two image footprints intersects the bbox.
        Footprints are derived from the GCPs in the TIFFs themselves.
        """
        if all(v is None for v in [lon_min, lon_max, lat_min, lat_max]):
            return True
        try:
            from geo_utils import get_gcps_for_folder, image_footprint, compute_overlap
            _, gcps_a = get_gcps_for_folder(self.folder_before)
            _, gcps_b = get_gcps_for_folder(self.folder_after)
            fp_a = image_footprint(gcps_a)
            fp_b = image_footprint(gcps_b)
            overlap = fp_a.intersection(fp_b)
            if overlap.is_empty:
                return False
            b = box(
                lon_min if lon_min is not None else -180,
                lat_min if lat_min is not None else -90,
                lon_max if lon_max is not None else 180,
                lat_max if lat_max is not None else 90,
            )
            return overlap.intersects(b)
        except Exception as e:
            print("overlap_intersects failed for " + self.label + ": " + str(e))
            return False


def get_pairs(db_path: str = DB_PATH,
              lon_min: Optional[float] = None,
              lon_max: Optional[float] = None,
              lat_min: Optional[float] = None,
              lat_max: Optional[float] = None) -> list[PairRecord]:
    """
    Fetch all pairs from the database, filter to those available locally,
    and optionally filter by overlap intersection with a coordinate bbox.
    """
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(
        'SELECT id_before, id_after FROM "' + TABLE + '"'
    ).fetchall()
    con.close()

    pairs = [PairRecord(id_before=r[0], id_after=r[1]) for r in rows]
    available = [p for p in pairs if p.available_locally]

    bbox_active = any(v is not None for v in [lon_min, lon_max, lat_min, lat_max])
    if bbox_active:
        available = [p for p in available
                     if p.overlap_intersects(lon_min, lon_max, lat_min, lat_max)]

    print("DB pairs: " + str(len(pairs)) + "  |  locally available: " + str(len(available)))
    return available