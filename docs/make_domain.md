# Create a Domain NetCDF with `utils/make_domain.py`

`utils/make_domain.py` builds a **domain NetCDF** (DEM + D8 + CN + optional channel mask)
from existing rasters/NetCDF inputs. It can crop to a bounding box, resample to a
requested resolution, and compute D8 flow directions directly from the DEM when
no D8 input is provided.

Run the utility multiple times (or via a single `--nested-config` hierarchy) to prepare **nested domains**
(e.g., 90 m national, 30 m regional, 10 m city) and list them under the `domains` array in `config.json`.
Each domain will be simulated independently with consistent hydrological/numerical settings and the same
MPI/shared-memory parallelization layout.

---

## 1. What it Produces

The output NetCDF contains:

- `dem` (float): Digital Elevation Model
- `d8` (int): D8 flow direction (ESRI encoding: 1,2,4,8,16,32,64,128)
- `cn` (float): SCS Curve Number (0–100)
- `channel_mask` (optional, int): 0/1 mask for channels

All variables use dimensions `(latitude, longitude)` and include CF-style
coordinate metadata. For CDL compliance, the output also includes a `crs`
grid-mapping variable (`grid_mapping_name`, `epsg_code`, `semi_major_axis`,
`inverse_flattening`) and each spatial variable references it via
`grid_mapping = "crs"`.

---

## 2. Inputs and Requirements

### Required input
- A DEM NetCDF (or GeoTIFF) with a 2D variable (default name: `dem`).

### Optional inputs
- D8 flow direction (`--d8`) NetCDF/GeoTIFF (default var: `d8`).
- Curve Number (`--cn`) NetCDF/GeoTIFF (default var: `cn`).
- Channel mask (`--mask`) NetCDF/GeoTIFF (default var: `channel_mask`).

### Optional dependencies
- **GeoTIFF support**: requires `rioxarray` + `rasterio`.
- **Linear interpolation** (used when `--resolution` is set): requires `scipy`.
  If `scipy` is unavailable, use native resolution or avoid `--resolution`.

---

## 3. Command-Line Usage

```bash
python utils/make_domain.py \
  --dem dem.nc \
  --cn cn.nc \
  --d8 d8.nc \
  --mask channel_mask.nc \
  --output domain.nc
```

### Options

- `--dem` **(required)**: DEM NetCDF/GeoTIFF path.
- `--cn`: CN NetCDF/GeoTIFF path (optional).
- `--d8`: D8 NetCDF/GeoTIFF path (optional; computed from DEM if omitted).
- `--mask`: Channel mask NetCDF/GeoTIFF path (optional).
- `--dem-var`: DEM variable name (default: `dem`).
- `--cn-var`: CN variable name (default: `cn`).
- `--d8-var`: D8 variable name (default: `d8`).
- `--mask-var`: Mask variable name (default: `channel_mask`).
- `--longitude-name`: Longitude coordinate name (default: `longitude`).
- `--latitude-name`: Latitude coordinate name (default: `latitude`).
- `--domain-name`: Label for the root domain (default: `root`); useful when referring to parents in nested configs.
- `--bbox`: Bounding box `min_lon min_lat max_lon max_lat`.
- `--resolution`: Target resolution `dx [dy]` (single value applies to both).
- `--nested-config`: JSON file describing nested domains (name, bbox, refinement > 1, output, optional parent).
- `--output` **(required)**: Output NetCDF path.

---

## 4. Examples

### 4.1 Minimal (compute D8 and set CN to zeros)
```bash
python utils/make_domain.py \
  --dem dem.nc \
  --output domain.nc
```

### 4.2 With CN and D8 inputs
```bash
python utils/make_domain.py \
  --dem dem.nc \
  --cn cn.nc \
  --d8 d8.nc \
  --output domain.nc
```

### 4.3 Crop to a bounding box and resample resolution
```bash
python utils/make_domain.py \
  --dem dem.nc \
  --cn cn.nc \
  --bbox 12.0 45.0 12.5 45.5 \
  --resolution 0.001 \
  --output domain_subset.nc
```

### 4.4 Use GeoTIFF inputs
```bash
python utils/make_domain.py \
  --dem dem.tif \
  --cn cn.tif \
  --mask channel_mask.tif \
  --output domain.nc
```

### 4.5 Build a nested hierarchy in one run
Create a JSON file listing nested domains, each with a bounding box, a refinement factor (child resolution = parent resolution ÷ refinement), and an optional parent. The root domain is always built from the CLI arguments.

```json
{
  "domains": [
    {
      "name": "regional_30m",
      "parent": "root",
      "bbox": [12.0, 45.0, 12.6, 45.6],
      "refinement": 3,
      "output": "domain_30m.nc"
    },
    {
      "name": "city_10m",
      "parent": "regional_30m",
      "bbox": [12.2, 45.1, 12.4, 45.3],
      "refinement": 3,
      "output": "domain_10m.nc"
    }
  ]
}
```

```bash
python utils/make_domain.py \
  --dem dem.nc \
  --bbox 11.0 44.5 13.0 46.0 \
  --resolution 90 \
  --nested-config nested_domains.json \
  --domain-name root \
  --output domain_90m.nc
```

Nested bounding boxes are snapped to parent grid lines and validated to stay inside the parent domain so particles can enter/exit nested grids cleanly.

---

## 5. Notes and Tips

- If you provide `--d8`, it is **regridded** to the DEM grid using nearest-neighbor.
- If you omit `--d8`, the script **computes D8** from the DEM using the ESRI encoding.
- CN defaults to all zeros if `--cn` is omitted.
- Ensure all inputs share the **same CRS and orientation** for best results.
- Use `--bbox` first to reduce domain size, then `--resolution` to resample.
