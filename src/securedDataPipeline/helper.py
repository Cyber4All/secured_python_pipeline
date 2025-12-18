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


def parse_ISO8601(col_name: str) -> pl.Expr:
    """
    Return a polars expression that parses string column that represent date to be of Date object
    """

    def normalize_dates(date: str) -> str:
        from re import sub

        """
        Some of the data entries in our CLARK database have erroneous dates,
        so some string date format isn't parseable by pendulum: 2018-03-13T17:13:12.000000+00:00Z
        There's a need to convert it to: 2018-09-14T11:53:41.812000Z so that pendulum can parse it
        Basically removing the +00:00 part
        """
        return sub(r"(\+\d{2}:\d{2})Z$", r"\1", date)

    parse_date_exp = pl.col(col_name).map_elements(
        lambda d: pm.parse(normalize_dates(d)).date(), return_dtype=pm.Date
    )
return parse_date_exp


