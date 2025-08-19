import polars as pl
from pymongo import MongoClient
from securedDataPipeline.helper import objID_to_string
from bson.objectid import ObjectId
from datetime import datetime

# pyarrow types
from pyarrow import field, list_, string, struct, int32
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all

from typing import List

patch_all()
client = MongoClient("mongodb://localhost:27017")

# Databases
# TODO: Determine if I should split the file by databases or even down to collections
onion_db = client["onion"]
cart_db = client["cart-service"]
topics_db = client["topics"]
sg_db = client["standard-guidelines"]
card_db = client["CARD"]

# Collections
objects_index_col = onion_db["objects-index"]
users_col = onion_db["users"]
downloads_col = onion_db["downloads"]
topics_col = topics_db["object-topics"]
tags_col = sg_db["tags"]
ratings_col = onion_db["ratings"]
submissions_col = onion_db["submissions"]
cae_orgs_col = card_db["organizations"]
collections_col = onion_db["collections"]
card_user_col = card_db["users"]

def get_collections():
    collections_df = collections_col.find_polars_all({})
    return collections_df

def get_users() -> pl.DataFrame:
    users_df = users_col.find_polars_all(
        {},
        projection={
            "Name": "$name",
            "Email": "$email",
            "Org": "$organization",
            "accessGroups": "$accessGroups",
            "createdAt": "$createdAt",
        },
    )

    return users_df


# Maps attributes of type list with their respective names from different collections
topics_df = objID_to_string(df=topics_col.find_polars_all({}), col="_id")
topicID_to_name = topics_df.select([pl.col("_id"), pl.col("name")]).to_dict(
    as_series=False
)
topic_dict = dict(zip(topicID_to_name["_id"], topicID_to_name["name"]))

tags_df = objID_to_string(df=tags_col.find_polars_all({}), col="_id")
tagsID_to_name = tags_df.select([pl.col("_id"), pl.col("name")]).to_dict(
    as_series=False
)
tag_dict = dict(zip(tagsID_to_name["_id"], tagsID_to_name["name"]))

orgs_df = objID_to_string(df=cae_orgs_col.find_polars_all({}), col="_id")
orgID_to_name = orgs_df.select([pl.col("_id"), pl.col("name")])
org_dict = dict(zip(orgID_to_name["_id"], orgID_to_name["name"]))

def map_topics(ids) -> List[str]:
    if pl.Series(ids).is_empty():
        return ["No Topic"]

    return [topic_dict.get(id, f"Unknown Topic ({id})") for id in ids]

def map_tags(ids) -> List[str]:
    return [tag_dict.get(id, f"Unknown Tag ({id})") for id in ids]

def map_card_orgs(ids) -> List[str]:
    return [org_dict.get(id, f"Unknown Organization ({id})") for id in ids]

def get_LO() -> pl.DataFrame:
    """
    Maps out tags and topics with their respective names into a learning objects dataframe

    Returns a dataframe of onion.objects, excluding _id
    """
    # Read in learning objects
    objects_index_df = (
        objects_index_col.find_polars_all(
            {},
            schema=Schema(
                {
                    "cuid": string(),
                    "topics": list_(string()),
                    "status": string(),
                    "date": string(),
                    "author": struct(
                        [
                            field("username", string()),
                            field("email", string()),
                            field("name", string()),
                        ]
                    ),
                    "contributors": list_(
                        struct(
                            [
                                field("name", string()),
                                field("organization", string()),
                                field("email", string()),
                            ]
                        )
                    ),
                    "objectCollection": string(),
                    "name": string(),
                    "version": int32(),
                    "id": string(),
                    "length": string(),
                    "tags": list_(string()),
                }
            ),
        )
        # Map the ID's from tags and topics to their respective names
        .with_columns(
            pl.col("topics").map_elements(map_topics, return_dtype=pl.List(pl.String)),
            pl.col("tags").map_elements(map_tags, return_dtype=pl.List(pl.String)),
        )
        .rename({"objectCollection": "collection"})
    )

    return objects_index_df


def get_downloads() -> pl.DataFrame:
    """
    Returns a dataframe of onions.downloads, excluding _id
    """
    downloads_df = objID_to_string(
        df=downloads_col.find_polars_all(
            {},
            projection={
                "Timestamp": "$timestamp",
                "cuid": "$learningObject.cuid",
                "downloadedBy": "$downloadedBy",
            },
        ).select(pl.exclude("_id")),
        col="downloadedBy",
    )

    return downloads_df


def get_submissions() -> pl.DataFrame:
    """
    Returns a dataframe of onion.submissions, excluding _id
    """
    submissions_df = submissions_col.find_polars_all(
        {},
        projection={
            "lo_id": "$learningObjectId",
            "collection": "$collection",
            "timestamp": "$timestamp",
        },
    ).select(pl.exclude("_id"))

    return submissions_df


def get_CAE_orgs() -> pl.DataFrame:
    """
    Returns a dataframe of CARD.organizations, excluding _id
    """
    cae_orgs_df = cae_orgs_col.find_polars_all(
        {},
        projection={
            "Name": "$name",
            "Type": "$type",
        },
    ).select(pl.exclude("_id"))

    return cae_orgs_df

def get_card_users() -> pl.DataFrame:
    """
    Returns a dataframe of CARD.users, excluding _id
    """
    card_users_df = (
        card_user_col.find_polars_all({}, projection={
            "Name": "$name",
            "Email": "$email",
            "Organization": "$organization",
            "Access": "$accessGroups"
        })
        .with_columns([
            # Convert ObjectId to string
            pl.col("_id")
            .map_elements(
                lambda o: ObjectId(o).generation_time, return_dtype=datetime).alias("createdAt"),

            # Map organization IDs to names
            pl.col("Organization")
            .map_elements(
                lambda org_id: org_dict.get(org_id, f"Unknown Organization ({org_id})"), return_dtype=pl.String)
            ])
    )

    return card_users_df

def get_card_resources() -> pl.DataFrame:
    """
    Returns a dataframe of CARD.resources
    """
    card_resources_df = card_db["resources"].find_polars_all(
        {},
        projection={
            "Name": "$name",
            "Status": "$status",
            "URL": "$url",
            "Notes": "$notes",
            "Organizations": "$organizations",
            "Category": "$category",
        },
    ).with_columns(
        pl.col("Organizations").map_elements(map_card_orgs, return_dtype=pl.List(pl.String))
    ).select(pl.exclude("_id"))

    return card_resources_df