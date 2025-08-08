csv_path = "../data/sample_data.csv"   # change if needed
lon_src   = "d_longitude"
lat_src   = "d_latitude"


# ---- 1) Load & clean ----
df = pd.read_csv(csv_path)

# keep rows with at least one positive coord (your rule), then coerce to float
df = df.query(f'{lon_src} > 0 | {lat_src} > 0').copy()
df["longitude"] = pd.to_numeric(df[lon_src], errors="coerce")
df["latitude"]  = pd.to_numeric(df[lat_src], errors="coerce")

# drop NaNs and out-of-range coords
df = df.dropna(subset=["longitude", "latitude"])
df = df[(df["longitude"] >= -180) & (df["longitude"] <= 180) &
        (df["latitude"]  >=  -90) & (df["latitude"]  <=  90)]

# short-circuit if nothing valid
if df.empty:
    raise ValueError("No valid rows after cleaning coordinates.")

# ---- 2) Build Spatially Enabled DF (WGS84) ----
sdf = GeoAccessor.from_xy(df, x_column="longitude", y_column="latitude", sr=4326)

# ---- 3) Align to layer schema ----
# get allowed fields from target layer
layer_fields = {f["name"] for f in layer.properties.fields}
protected = {"OBJECTID"}
gid = (layer.properties.get("globalIdField") or "").strip()
if gid:
    protected.add(gid)

# only keep attributes that exist on the layer and are not protected
keep_cols = [c for c in sdf.columns if (c in layer_fields) and (c not in protected)]

# you can map CSV names to layer names here if needed, e.g.:
# rename_map = {"full_name": "fullname", "phone_no": "phone"}
# sdf = sdf.rename(columns=rename_map)
# then recompute keep_cols if you renamed:
# keep_cols = [c for c in sdf.columns if (c in layer_fields) and (c not in protected)]

# ---- 4) Append in batches as a FeatureSet ----
def adds_batch(sdf_batch):
    # build featureset: attributes + geometry (SHAPE comes from sdf.spatial)
    cols = keep_cols + ["SHAPE"]
    fs = sdf_batch[cols].spatial.to_featureset()
    res = layer.edit_features(adds=fs)
    return res

batch_size = 1000
n = len(sdf)
print(f"Preparing to add {n} features to layer: {layer.properties.name}")

start = 0
total_added = 0
errors = []

while start < n:
    end = min(start + batch_size, n)
    print(f"Adding features {start}–{end-1} ...")
    result = adds_batch(sdf.iloc[start:end].copy())

    add_results = result.get("addResults", [])
    total_added += sum(1 for r in add_results if r.get("success"))
    batch_errors = [r.get("error") for r in add_results if not r.get("success")]
    if batch_errors:
        errors.extend(batch_errors)
        print(f"  Warnings: {len(batch_errors)} failures in this batch")

    start = end

print(f"Append complete. Successfully added: {total_added}/{n}")
if errors:
    print(f"{len(errors)} adds failed. First few errors:")
    for e in errors[:5]:
        print("  -", e)

# ---- 5) Verify feature count ----
count_after = layer.query(return_count_only=True)
print("Layer feature count now:", count_after)