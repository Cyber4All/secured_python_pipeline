# SecurEd Python Data Pipeline

A python module that takes in data from Google Analytics and joins the appropriate MongoDB SecurEd collections as [Polars](https://pola.rs/) dataframes for easier data analysis.

The dataframe can be converted to different data types and file formats such as csv, Pandas dataframe, [Apache Arrow](https://arrow.apache.org/), etc.

The version of PyMongo used throughout this modules leverages the [PyMongoArrow](https://www.mongodb.com/developer/languages/python/pymongoarrow-and-data-analysis/) extension.

> PyMongoArrow is a PyMongo extension containing tools for loading MongoDB query result sets as Apache Arrow tables, Pandas and NumPy arrays

## Requirements

A Google Analytics account to supply the following environment variables to be written in a `.env` file:

- GOOGLE_SERVICE_ACCOUNT_EMAIL
- GOOGLE_PRIVATE_KEY

## Importing

This is a private repo, so you'll need to upload your public ssh key to GitHub for installation to work

### uv

`uv add git+ssh://git@github.com/Cyber4All/secured_python_pipeline.git`

### pip

`pip install git+ssh://git@github.com/Cyber4All/secured_python_pipeline.git`
