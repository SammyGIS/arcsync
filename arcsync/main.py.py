from pipeline.spatial_etl import run_spatial_etl

my_feature_layer = ""
df = ""

run_spatial_etl(
    df= df,
    layer= my_feature_layer,
    sr = 4326,
    batch_size = 1000
) 