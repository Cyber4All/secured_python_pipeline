import polars as pl
from bson.objectid import ObjectId

from dotenv import load_dotenv
from os import environ




def get_browseViews(startDate: str = "2015-08-14", endDate: str = "today"):
    from urllib.parse import parse_qs, urlparse

    """
    Gets the user behavior when they browse learning objects
    """

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
                # Filter out routes without queries
                pl.col("url").str.contains(r"\?")
            )
            .select(["url", "visits"])
            .with_columns(
                # query params to dictionary
                pl.col("url").map_elements(
                    lambda u: parse_qs(urlparse(u).query),
                    return_dtype=pl.Struct,
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


def get_pageViews(startDate: str, endDate: str) -> pl.DataFrame:
    """
    Gets the page views of learning objects via Google Analytics

    Args:
    - startDate: Starting date
    - endDate: ending date
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
        return

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
