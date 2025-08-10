import pandas as pd

def load_and_filter_csv(csv_path: str, lon_src: str, lat_src: str) -> pd.DataFrame:
    """Load CSV, filter by coordinate rule, and convert to numeric."""
    df = pd.read_csv(csv_path)
    df = df.query(f'{lon_src} > 0 | {lat_src} > 0').copy()
    df["longitude"] = pd.to_numeric(df[lon_src], errors="coerce")
    df["latitude"]  = pd.to_numeric(df[lat_src], errors="coerce")
    return df

def clean_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with NaNs or out-of-range coordinates."""
    df = df.dropna(subset=["longitude", "latitude"])
    df = df[(df["longitude"] >= -180) & (df["longitude"] <= 180) &
            (df["latitude"]  >=  -90) & (df["latitude"]  <=  90)]
    if df.empty:
        raise ValueError("No valid rows after cleaning coordinates.")
    return df