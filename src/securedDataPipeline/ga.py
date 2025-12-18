import polars as pl
from dotenv import load_dotenv, find_dotenv
from securedDataPipeline.mongo import get_LO, map_topics, map_tags, objects_index_col, get_collections
from pyarrow import field, string, struct
from os import environ
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    Filter,
    FilterExpression,
    FilterExpressionList,
)
from google.api_core.exceptions import InvalidArgument
from pymongoarrow.api import Schema
from google.protobuf.json_format import MessageToDict

load_dotenv(find_dotenv())

def get_formatted_private_key():
    """Properly format the Google private key from environment variable"""
    private_key = environ.get('GOOGLE_PRIVATE_KEY', '')
    
    # Remove any existing header/footer
    private_key = private_key.replace('-----BEGIN PRIVATE KEY-----', '')
    private_key = private_key.replace('-----END PRIVATE KEY-----', '')
    
    # Replace literal \n with actual newlines
    private_key = private_key.replace('\\n', '\n')
    
    # Remove any whitespace
    private_key = private_key.strip()
    
    # Reconstruct with proper formatting
    return f"-----BEGIN PRIVATE KEY-----\n{private_key}\n-----END PRIVATE KEY-----"

ga4_creds = {
    "client_email": environ.get("GOOGLE_SERVICE_ACCOUNT_EMAIL"),
    "private_key": get_formatted_private_key(),
    "token_uri": "https://oauth2.googleapis.com/token",
}

ga4_client = BetaAnalyticsDataClient.from_service_account_info(ga4_creds)
property_id = "332215249"


def getBrowsePageViews(startDate: str = "2015-08-14", endDate: str = "today"):
    """
    Gets the user behavior when they browse learning objects

    Parameters
    ----------
    startDate: str
        - Start date for the query
    endDate: str
        - End date for the query

    See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange for valid DateRange values
    Returns
    -------
    pl.DataFrame with the following columns:
            - url: The URL of the page visited
            - visits: The number of visits to that page
            - topics: List of topics associated with the page
            - tags: List of tags associated with the page
    """
    from urllib.parse import parse_qs, urlparse

    try:
        browse_req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="pagePathPlusQueryString")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=startDate, end_date=endDate)],
            dimension_filter=FilterExpression(
                and_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            filter=Filter(
                                field_name="pagePathPlusQueryString",
                                # Only get routes with browse path
                                string_filter=Filter.StringFilter(
                                    value="browse",
                                    match_type=Filter.StringFilter.MatchType(4),
                                ),
                            )
                        ),
                        FilterExpression(
                            # Filter out local development statistics
                            not_expression=FilterExpression(
                                filter=Filter(
                                    field_name="pagePathPlusQueryString",
                                    string_filter=Filter.StringFilter(
                                        value="localhost",
                                        match_type=Filter.StringFilter.MatchType(4),
                                    ),
                                )
                            )
                        ),
                    ]
                )
            ),
            limit=200_000,
        )
    except InvalidArgument as e:
        print(e)
        return

    ga4_res = MessageToDict(ga4_client.run_report(browse_req)._pb)
    # Return empty DataFrame if no rows exist in the response
    if "rows" not in ga4_res:
        return pl.DataFrame()

    url_df = (
        (
            pl.DataFrame(ga4_res["rows"])
            .with_columns(
                [
                    pl.col("dimensionValues").explode().struct[0].alias("url"),
                    pl.col("metricValues")
                    .explode()
                    .struct[0]
                    .cast(pl.Int32)
                    .alias("visits"),
                ]
            )
            .filter(
                # Filter out routes with unnecessary queries
                [
                    # No queries provided
                    pl.col("url").str.contains(r"\?"),
                    # URL not encoded properly
                    ~pl.col("url").str.contains(r"amp;"),
                    # For translate this page to 'x' language
                    ~pl.col("url").str.contains(r"[\?&]_x_")
                ]
            )
            .select(["url", "visits"])
            .with_columns(
                # query params to dictionary
                pl.col("url").map_elements(
                    lambda u: parse_qs(urlparse(u).query),
                    # TODO: Couldn't satisfy the proper return data type. Will have to revisit later
                    # return_dtype=pl.Struct,
                ),
            )
            # create a column for each query parameter
            .unnest("url")
        )
        .explode("text")
        .explode("currPage")
    )

    if "topics" in url_df.columns:
        try:
            url_df = url_df.with_columns(
                pl.col("topics").map_elements(
                    map_topics, return_dtype=pl.List(pl.String)
                )
            )
        except pl.exceptions.ComputeError as err:
            print(err)

    if "tags" in url_df.columns:
        try:
            url_df = url_df.with_columns(
                pl.col("tags").map_elements(map_tags, return_dtype=pl.List(pl.String))
            )
        except pl.exceptions.ComputeError as err:
            print(err)

    return url_df


def getDetailPageViews(startDate: str = "2015-08-14", endDate: str = "today") -> pl.DataFrame | None:
    """
    Gets the page views of learning objects via Google Analytics

    Parameters
    ----------
    startDate: str
        - Starting date for the query
    endDate: str
        - Ending date for the query

    See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange for valid DateRange values
    Returns
    -------
    pl.DataFrame with the following columns:
        - lo_cuid: The CUID of the learning object
        - views: The number of views for that learning object
    """

    authors = objects_index_col.find_polars_all(
        {"status": "released"},
        schema=Schema(
            {
                "cuid": string(),
                "author": struct(
                    [
                        field("username", string()),
                    ]
                ),
            }
        ),
    ).unnest("author")

    # Create URL paths for learning objects
    url_path = []
    for cuid, author in authors.iter_rows():
        url_path.append(f"/details/{author}/{cuid}")
    url_path = pl.Series(url_path)

    try:
        # GA4 report for learning object page views
        views_req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="pagePath")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=startDate, end_date=endDate)],
            # Get the reports from learning object links
            dimension_filter=FilterExpression(
                or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(
                            filter=Filter(
                                field_name="pagePath",
                                string_filter=Filter.StringFilter(
                                    value=path,
                                    match_type=Filter.StringFilter.MatchType(4),
                                ),
                            )
                        )
                        for path in url_path
                    ]
                )
            ),
        )
    except InvalidArgument as e:
        print(e)
        return None

    ga4_res = MessageToDict(ga4_client.run_report(views_req)._pb)
    # Return empty DataFrame if no rows exist in the response
    if "rows" not in ga4_res:
        return pl.DataFrame()

    # Protobuf to dictionary to Dataframe
    views_df = pl.DataFrame(ga4_res["rows"])
    views_df = (
        (
            views_df.with_columns(
                [
                    # URL Path
                    pl.col("dimensionValues")
                    .explode()
                    .struct.unnest()
                    .alias("lo_cuid"),
                    # Page views
                    pl.col("metricValues")
                    .explode()
                    .struct.unnest()
                    .alias("views")
                    .cast(pl.Int32),
                ]
            )
            .select(["lo_cuid", "views"])
            # Filter out error pages
            .filter(~pl.col("lo_cuid").str.contains("/unauthorized"))
        )
        .with_columns(
            # Only keep the cuid from the url path. Omit the rest
            pl.col("lo_cuid").str.split("/").list.get(3)
        )
        # Disregard version and just add up all of their views
        .group_by("lo_cuid")
        .agg(pl.col("views").sum())
        # Join with LO table
    ).join(get_LO(), left_on="lo_cuid", right_on="cuid")


    return views_df

def getCollectionPageViews(startDate: str = "2015-08-14", endDate: str = "today") -> pl.DataFrame | None:
    """
    Gets the page views of a collections page via Google Analytics

    Parameters
    ----------
    startDate: str
        - Starting date for the query
    endDate: str
        - Ending date for the query

    See https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/DateRange for valid DateRange values
    Returns
    -------
    pl.DataFrame with the following columns:
        - collection: The name of the collection
        - views: The number of views for that collection
    """

    collections = get_collections()["abvName"].to_list()

    # Store all of the links of collection pages within a regex
    collection_group = "|".join(collections)
    # Some collection pages have /c/ and /collections/ in their path
    regex_pattern = (
        fr"|^/c/({collection_group})$"
        fr"|^/collections/({collection_group})$"
    )
    try:
        views_req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="pagePath")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=startDate, end_date=endDate)],
            dimension_filter=FilterExpression(
                filter=Filter(
                   field_name="pagePath",
                   string_filter=Filter.StringFilter(
                       value=regex_pattern,
                       match_type=Filter.StringFilter.MatchType(5),
                       case_sensitive=True
                   ),
               )
            ),
        )
    except InvalidArgument as e:
        print(e)
        return None

    ga4_res = MessageToDict(ga4_client.run_report(views_req)._pb)
    # Return empty DataFrame if no rows exist in the response
    if "rows" not in ga4_res:
        return pl.DataFrame()

    # Protobuf to dictionary to Dataframe
    views_df = pl.DataFrame(ga4_res["rows"])

    # Get dataframe of collection page views
    return views_df.with_columns([
        (pl.col("dimensionValues").list.get(0).struct.field("value").str.split("/").list.get(-1).alias("collection")),

        # Get the number of views and cast as int
        pl.col("metricValues").list.get(0).struct.field("value").alias("views").cast(pl.Int32),
    ]).select(["collection", "views"])
