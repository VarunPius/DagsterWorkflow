######################################################################################################################################################
# Code Info                                                                                                                                          #
#                                                                                                                                                    #
# stock_price_assets.py                                                                                                                              #
# Author(s): Varun Pius Rodrigues                                                                                                                    #
# About: Define assets for getting historical stock price and getting their aggregates                                                               #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
#                                                                                                                                                    #
# Version Log:                                                                                                                                       #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Version ID | Changes                                                                                                                               #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
#   v0.0.0   | StockPriceWorkflow: Setting up Dagster project                                                                                        #
#   v0.0.1   | StockPriceWorkflow: Adding comments and description                                                                                   #
#   v1.0.0   | StockPriceWorkflow: Getting Stock price from sample tickers                                                                           #
#   v2.0.0   | StockPriceWorkflow: Calculating stock price aggregates                                                                                #
#   v3.0.0   | StockPriceWorkflow: Creating jobs                                                                                                     #
#   v4.0.0   | StockPriceWorkflow: Changing assets to pass dataframe between assets                                                                  #
#   v5.0.0   | StockPriceWorkflow: Final version if multiple jobs, assets configured                                                                 #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
import sys
import os
from datetime import datetime
import logging
import tempfile
import json

# Internal imports
# None

# External librabries
import pandas as pd
import requests
import yfinance as yf

from dagster import asset, AssetIn, AssetExecutionContext
from dagster import MaterializeResult, MetadataValue


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Configurations:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #



# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Workflow definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

@asset(key="price_asset", group_name="grp_stock_agg")
def get_stock_price(context: AssetExecutionContext) -> pd.DataFrame:
    context.log.info("Getting Stock Price")
    tickers = [
        'AAPL', 
        'CRM' ,
        'GOOG',
        'META',
        'MSFT',
        'NFLX',
        'NVDA',
    ]

    company_price_list = []
    for company in tickers:
        context.log.info("Currently processing: {0}".format(company))
        company_obj = yf.Ticker(company)
        df = company_obj.history(start='2024-01-01', end='2024-04-15')
        
        df['ticker'] = company
        # df['date'] = df.index     # Assigning new column with same data as index, which is date
        df = df.reset_index()       # Resetting index moves the index to a column and new index will be incremental
        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
        context.log.info("Raw Data for {0}".format(company))
        context.log.info(df)

        context.log.info("Done fetching data for: {0}".format(company))
        context.log.info("Dataframe columns: {0}".format(df.columns))
        
        df = df[['ticker', 'date', 'Close']]        # filtering only necessary columns

        company_price_list.append(df)

    context.log.info("Done fetching all data")

    result = pd.concat(company_price_list)
    result = result.reset_index()           # resetting index to be incremental rather than resetting for every company
    result.drop(['index'], axis=1)          # index from previous result moves to a column after resetting

    context.log.info("Final dataframe: ")
    context.log.info(result)

    context.log.info("Writing the data to asset 1:")
    os.makedirs("DataPipelines/data", exist_ok=True)
    result.to_csv('DataPipelines/data/asset1.csv')

    context.log.info("Done")
    return result

# Alternative way to write when not passing any data
# @asset(deps=[topstory_ids], group_name="hackernews", compute_kind="HackerNews API")
@asset(
    ins={"upstream_df": AssetIn(key="price_asset")},
    group_name="grp_stock_agg"
    #deps=[get_stock_price]
)
def eval_stock_agg(context: AssetExecutionContext, upstream_df: pd.DataFrame) -> MaterializeResult:

    context.log.info("Aggregation of stock price")
    
    context.log.info("Reading data:")
    #stock_price_df = pd.read_csv("data/asset1.csv")
    stock_price_df = upstream_df
    
    context.log.info("Grouping By / Creating Groups")
    temp_output = stock_price_df.groupby('ticker').agg({'Close': 'mean'})
    
    agg_output = temp_output.reset_index()
    
    context.log.info("Aggregated output: ")
    context.log.info(agg_output)
    
    context.log.info("Writing the data to asset 2:")
    agg_output.to_csv('DataPipelines/data/asset2.csv')
    return MaterializeResult(
        metadata = {
            "num_of_companies": len(agg_output.index),
            "preview": MetadataValue.md(agg_output.head().to_markdown()),
        }
    )


