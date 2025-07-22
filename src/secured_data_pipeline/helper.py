import polars as pl
from bson.objectid import ObjectId


def objID_to_string(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Convert binary ObjectID into its string value
    Args:
        df: pl.DataFrame
            - Dataframe to convert it's column from objectID with
        col: str
            - The column to convert from objectID to string
    """
    return df.with_columns(
        [
            pl.col(col)
            .cast(pl.Binary)
            .map_elements(lambda o: str(ObjectId(o)), return_dtype=pl.String)
        ]
    )
