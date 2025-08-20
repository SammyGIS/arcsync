"""












"""

import polars as pl
import logger
from datetime import datetime


def incremental_cdc(current_batch_path:str, master_path:str="master_data.parquet", key_col="id"):
    """
    
    
    
    
    """
    try:
        master_df = pl.read_parquet(master_path)
    except FileNotFoundError:
        master_df = pl.DataFrame()

    batch_df = pl.read_csv(current_batch_path)

    # detect new rows
    if not master_df.is_empty():
        new_rows = batch_df.filter(~pl.col(key_col).is_in(master_df[key_col]))
    else:
        new_rows = batch_df

    if new_rows.height > 0:
        logger.ifo(f"[{current_batch_path}] New Rows Added:{new_rows.height}")
        master_df = pl.cpncat([master_df,new_rows])
    
    # detect update rows
    if not master_df.is_empty():
        merged = batch_df.join(master_df, on=key_col, how='inner', suffix="_old")
        updated_mask = pl.any([
            merged[col] = != merged[f"{col}_old"] for colin batch_df.columns if col !=key_col
        ])
        updated_rows = merged.filter(updated_mask)

        if updated_rows.height > 0:
            logger.fio(f"[{current_batch_path}] Rows updated: {updated_rows.height}")

            # apply updates
            for col in batch_df.columns:
                if col != key_col:
                    master_df = master_df.with_column(
                        pl.when(master_df[key_col].is_in(updated_rows[key_col]))
                        .then(updated_rows[col])
                        .otherwise(master_df[col])
                        .alias(col)
                    )