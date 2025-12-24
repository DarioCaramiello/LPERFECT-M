# `utils/output_to_geo.py` — LPERFECT NetCDF → GeoJSON enrichment

This utility reads the **LPERFECT** NetCDF output compliant with `cdl/output_flood_depth.cdl` and enriches a **GeoJSON FeatureCollection** with per-feature flood/risk metrics.

It is designed to support **point** and **non-point** features:
- **Points**: nearest-neighbor sampling on the NetCDF grid (or optional point buffering to treat points as small areas).
- **Polygons / MultiPolygons**: **accurate area-weighted mean** and **maximum** computed over the intersected grid cells.
- **Lines / MultiLines**: optionally buffered (meters) into corridors, then treated as areas (recommended). If not buffered, centroid sampling is used.

Additionally, it can compute **percent area above a flood depth threshold**.

---

## What it reads

### NetCDF (LPERFECT output)
Expected variables/dimensions (from the CDL):
- Dimensions: `time`, `latitude`, `longitude`
- Variables:
  - `latitude(latitude)` (degrees_north)
  - `longitude(longitude)` (degrees_east)
  - `flood_depth(time, latitude, longitude)` (meters)
  - `risk_index(time, latitude, longitude)` (unitless)
  - Optional `_FillValue` attribute on `flood_depth` and `risk_index`

### GeoJSON input
- Must be a `FeatureCollection`
- Each `Feature` may contain a `properties` object (it will be created if missing)

---

## What it produces

A GeoJSON `FeatureCollection` where each feature gets new properties (names configurable via CLI):

Default added properties:
- `flood_depth_mean` (float, meters) — area-weighted mean flood depth
- `flood_depth_max`  (float, meters) — maximum flood depth
- `risk_index_mean`  (float, unitless) — area-weighted mean risk index
- `risk_index_max`   (float, unitless) — maximum risk index
- `flood_depth_pct_gt_thr` (float, %) — percent of **valid intersected area** where flood depth is above threshold (if enabled)
- `risk_mode` (string) — indicates how the value was computed: `point`, `area`, `line_centroid`, `centroid`, ...

Notes:
- For **area statistics**, intersection is computed in an **equal-area CRS** (default `EPSG:6933`) to ensure areas are meaningful.
- The percent area is computed over **valid intersected area** (cells with valid `flood_depth`), excluding `_FillValue`/NaNs from the denominator.

---

## Installation

From your project root (or inside a venv):

```bash
pip install xarray netCDF4 numpy pyproj shapely
```

---

## Usage

### Basic (GeoJSON already in EPSG:4326)

```bash
python utils/output_to_geo.py \
  --nc output_flood_depth.nc \
  --geojson-in features.geojson \
  --geojson-out features_with_risk.geojson
```

### GeoJSON in a projected CRS (example: UTM 33N EPSG:32633)

```bash
python utils/output_to_geo.py \
  --nc output_flood_depth.nc \
  --geojson-in assets_utm33.geojson \
  --geojson-out assets_with_risk.geojson \
  --geojson-epsg 32633
```

### Compute percent area with depth > 0.20 m (Polygons; buffered lines)

```bash
python utils/output_to_geo.py \
  --nc output_flood_depth.nc \
  --geojson-in assets.geojson \
  --geojson-out assets_with_risk.geojson \
  --depth-threshold-m 0.20 \
  --line-buffer-m 8
```

### Treat points as small areas (buffer radius in meters)

```bash
python utils/output_to_geo.py \
  --nc output_flood_depth.nc \
  --geojson-in sensors.geojson \
  --geojson-out sensors_with_risk.geojson \
  --point-buffer-m 50 \
  --depth-threshold-m 0.10
```

---

## Command-line reference

### Inputs/outputs
- `--nc PATH` **(required)**: NetCDF file from LPERFECT
- `--geojson-in PATH` **(required)**: input GeoJSON FeatureCollection
- `--geojson-out PATH` **(required)**: output GeoJSON
- `--time-index N` (default `0`): index of `time` to use

### CRS
- `--geojson-epsg EPSG` (default `4326`): EPSG of input GeoJSON coordinates
- `--area-epsg EPSG` (default `6933`): equal-area CRS for intersection areas (m²)

### Geometry handling
- `--line-buffer-m METERS` (default `0`):
  - If > 0, LineString/MultiLineString are buffered into corridors and treated as polygons
  - If 0, lines are sampled at the centroid (less accurate; generally avoid)
- `--point-buffer-m METERS` (default `0`):
  - If > 0, Point geometries are buffered (circles) and treated as polygons

### Threshold metric
- `--depth-threshold-m METERS` (default `None`):
  - If set, adds `flood_depth_pct_gt_thr`

### Output property names (advanced)
You can rename output property keys:
- `--prop-flood-mean NAME` (default `flood_depth_mean`)
- `--prop-flood-max NAME`  (default `flood_depth_max`)
- `--prop-risk-mean NAME`  (default `risk_index_mean`)
- `--prop-risk-max NAME`   (default `risk_index_max`)
- `--prop-flood-pct NAME`  (default `flood_depth_pct_gt_thr`)
- `--prop-mode NAME`       (default `risk_mode`)

### Debugging
- `--add-grid-idx` (flag): for point sampling, store nearest `_lperfect_ilat`, `_lperfect_ilon`
- `--log-level {DEBUG,INFO,WARNING,ERROR}` (default `INFO`)

---

## How area statistics are computed (short explanation)

1. The feature geometry is reprojected from **EPSG:4326** to an **equal-area** CRS (default **EPSG:6933**).
2. Grid cell rectangles are inferred from the 1D `latitude`/`longitude` **centers** by creating **cell edges** (midpoints).
3. For each cell intersecting the feature:
   - Compute intersection area (m²)
   - Multiply values by area and accumulate for the mean
   - Track max
4. Percent area above threshold uses the ratio:

```text
100 * (sum of intersected areas where flood_depth > threshold)
    / (sum of intersected areas where flood_depth is valid)
```

---

## Output example snippet

```json
{
  "type": "Feature",
  "properties": {
    "name": "Bridge A",
    "flood_depth_mean": 0.12,
    "flood_depth_max": 0.41,
    "risk_index_mean": 0.38,
    "risk_index_max": 0.72,
    "flood_depth_pct_gt_thr": 18.4,
    "risk_mode": "area",
    "_depth_threshold_m": 0.2
  },
  "geometry": { "...": "..." }
}
```

---

## Performance tips

- Large grids + many polygons can be expensive. To keep running fast:
  - Prefer simpler geometries (simplify upstream if needed)
  - Use `--line-buffer-m` to convert lines to corridors (still polygonal but avoids centroid underestimation)
  - Consider splitting very large FeatureCollections into chunks and processing them in parallel

---

## Troubleshooting

### “Latitude/longitude must be strictly monotonic”
Your `latitude` or `longitude` array is not strictly increasing/decreasing, or is not 1D.
This script assumes a **rectilinear grid**. If you have a curvilinear grid (2D lon/lat),
a different cell geometry strategy is needed.

### All outputs are `null`
- Check that features overlap the NetCDF grid extent
- Check `_FillValue` is not dominating the grid
- Try `--log-level DEBUG`

### Percent area is `null`
Percent requires `--depth-threshold-m` and at least some **valid** flood depth in the intersected area.

---

## License / attribution
Use according to your project’s license. This README documents the behavior of `utils/output_to_geo.py`.
