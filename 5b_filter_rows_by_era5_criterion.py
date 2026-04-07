#!/usr/bin/env python3
import argparse
from pathlib import Path
import os
from typing import Callable

import duckdb
import polars as pl
import cdsapi
import xarray as xr
import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm


def pick_xarray_engine() -> str | None:
    try:
        import netCDF4  # noqa: F401
        return "netcdf4"
    except Exception:
        pass
    try:
        import h5netcdf  # noqa: F401
        return "h5netcdf"
    except Exception:
        pass
    return None


def ensure_era5_files(
    *,
    key_dotenv: str,
    era5_dir: str,
    dataset: str,
    variable: str,
    years: list[int],
    area: list[float],
    times: list[str],
    verbose: bool,
) -> dict[int, Path]:
    load_dotenv(key_dotenv)
    client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.getenv("API_KEY"))

    era5_dir = Path(era5_dir)
    safe_dataset = dataset.replace("/", "_")
    year_to_path: dict[int, Path] = {}

    for y in tqdm(years, desc="ERA5 years (ensure downloaded)", unit="year"):
        nc_path = era5_dir / f"{safe_dataset}__{variable}__{y}.nc"
        nc_path.parent.mkdir(parents=True, exist_ok=True)

        if not nc_path.exists():
            tqdm.write(f"Downloading ERA5 {dataset}/{variable} year={y} -> {nc_path}")
            client.retrieve(
                dataset,
                {
                    "product_type": ["reanalysis"],
                    "variable": [variable],
                    "year": [str(y)],
                    "month": [f"{m:02d}" for m in range(1, 13)],
                    "day": [f"{d:02d}" for d in range(1, 32)],
                    "time": times,
                    "data_format": "netcdf",
                    "download_format": "unarchived",
                    "area": area,
                },
            ).download(str(nc_path))
        else:
            if verbose:
                tqdm.write(f"Using cached {nc_path}")

        year_to_path[y] = nc_path

    return year_to_path


def make_comparator(op: str, threshold: float) -> Callable[[np.ndarray], np.ndarray]:
    if op == "lt":
        return lambda v: v < threshold
    if op == "le":
        return lambda v: v <= threshold
    if op == "gt":
        return lambda v: v > threshold
    if op == "ge":
        return lambda v: v >= threshold
    if op == "eq":
        return lambda v: v == threshold
    return lambda v: v != threshold  # ne


def filter_overlaps_batched(
    overlaps: pl.DataFrame,
    year_to_path: dict[int, Path],
    *,
    netcdf_var: str | None,
    op: str,
    threshold: float,
    batch_size: int,
    load_netcdf: bool,
    verbose: bool,
) -> pl.DataFrame:
    engine = pick_xarray_engine()
    if verbose:
        print(f"xarray engine: {engine or 'auto/default'}")

    cmpv = make_comparator(op, threshold)
    parts: list[pl.DataFrame] = []

    groups = list(overlaps.group_by(pl.col("point_datetime").dt.year()))
    for (year,), dfy in tqdm(groups, desc="Years (filtering)", unit="year"):
        year = int(year)
        nc_path = year_to_path[year]

        ds = xr.open_dataset(nc_path, engine=engine) if engine else xr.open_dataset(nc_path)
        if load_netcdf:
            ds = ds.load()

        var_name = netcdf_var or list(ds.data_vars)[0]
        da = ds[var_name]

        times = dfy["point_datetime"].to_numpy()
        lats = dfy["overlap_lat"].to_numpy().astype(np.float64)
        lons = dfy["overlap_lon"].to_numpy().astype(np.float64)

        keep = np.zeros(dfy.height, dtype=bool)

        for start in tqdm(
            range(0, dfy.height, batch_size),
            desc=f"{year} batches",
            unit="batch",
            leave=False,
        ):
            end = min(start + batch_size, dfy.height)

            t_da = xr.DataArray(times[start:end], dims=("points",))
            lat_da = xr.DataArray(lats[start:end], dims=("points",))
            lon_da = xr.DataArray(lons[start:end], dims=("points",))

            vals = da.sel(
                valid_time=t_da,
                latitude=lat_da,
                longitude=lon_da,
                method="nearest",
            ).values
            vals = np.asarray(vals).reshape(-1).astype(np.float64)

            ok = ~np.isnan(vals)
            keep[start:end] = ok & cmpv(vals)

        tqdm.write(f"Year {year}: kept {int(keep.sum())}/{dfy.height}")
        parts.append(dfy.filter(pl.Series(keep)))

        ds.close()

    return pl.concat(parts) if parts else overlaps.head(0)


def main() -> None:
    p = argparse.ArgumentParser(description="Filter rows by ERA5 value at overlap centroid (nearest, batched).")
    p.add_argument("--db", required=True)
    p.add_argument("--input-table", required=True)
    p.add_argument("--output-table", default=None)

    p.add_argument("--key-dotenv", required=True)
    p.add_argument("--era5-table", default="reanalysis-era5-single-levels")
    p.add_argument("--era5-variable", default = "sea_ice_cover")
    p.add_argument("--netcdf-var", default=None)

    p.add_argument("--op", default = "ge", choices=["lt", "le", "gt", "ge", "eq", "ne"])
    p.add_argument("--threshold", default=.15, type=float)

    p.add_argument("--era5-dir", default="intermediates/era5")
    p.add_argument("--area", default="-60,-180,-80,180", help="N,W,S,E")
    p.add_argument("--times", default="00:00,12:00")
    p.add_argument("--batch-size", type=int, default=25_000)

    p.add_argument(
        "--load-netcdf",
        action="store_true",
        default = True,
        help="Load each year's NetCDF fully into memory when processing that year.",
    )

    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--memory-limit", default="16GB")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    out_table = args.output_table or f"{args.input_table}__era5_filtered"

    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    overlaps = con.execute(f"""
        SELECT
            *,
            ST_X(ST_Centroid(geometry_overlap)) AS overlap_lon,
            ST_Y(ST_Centroid(geometry_overlap)) AS overlap_lat
        FROM {args.input_table}
    """).pl()

    overlaps = overlaps.with_columns(
        pl.col("point_datetime").cast(pl.Datetime, strict=False).alias("point_datetime")
    )

    min_year, max_year = overlaps.select(
        pl.col("point_datetime").dt.year().min().alias("min_year"),
        pl.col("point_datetime").dt.year().max().alias("max_year"),
    ).row(0)
    years = list(range(int(min_year), int(max_year) + 1))
    print(f"Data spans years: {years[0]} to {years[-1]}")

    area = [float(x) for x in args.area.split(",")]
    times = [t.strip() for t in args.times.split(",") if t.strip()]

    year_to_path = ensure_era5_files(
        key_dotenv=args.key_dotenv,
        era5_dir=args.era5_dir,
        dataset=args.era5_table,
        variable=args.era5_variable,
        years=years,
        area=area,
        times=times,
        verbose=args.verbose,
    )

    filtered = filter_overlaps_batched(
        overlaps,
        year_to_path,
        netcdf_var=args.netcdf_var,
        op=args.op,
        threshold=args.threshold,
        batch_size=args.batch_size,
        load_netcdf=args.load_netcdf,
        verbose=args.verbose,
    )

    filtered = filtered.drop(["overlap_lon", "overlap_lat"])
    con.execute(f"CREATE OR REPLACE TABLE {out_table} AS SELECT * FROM filtered")

    print(f"Created table: {out_table}")
    print(f"Total rows after filtering: {filtered.height}/{overlaps.height}")

    summary = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(summary.columns) + 1) // 2
    print(summary.iloc[:, :mid])
    print(summary.iloc[:, mid:])


if __name__ == "__main__":
    main()