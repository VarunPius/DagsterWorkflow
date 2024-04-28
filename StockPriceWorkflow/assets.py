######################################################################################################################################################
# Code Info                                                                                                                                          #
#                                                                                                                                                    #
# assets.py                                                                                                                                          #
# Author(s): Varun Pius Rodrigues                                                                                                                    #
# About: Define workflow in Dagster to get stock prices                                                                                              #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
#                                                                                                                                                    #
# Version Log:                                                                                                                                       #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Version ID | Changes                                                                                                                               #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
#   v0.0.0   | Setting up Dagster project                                                                                                            #
#   v0.0.1   | Adding comments and description                                                                                                       #
#   v1.0.0   | Getting Stock price from sample tickers                                                                                               #
#   v2.0.0   | Calculating stock price aggregates                                                                                                    #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
import sys
import os
#os.environ["no_proxy"] = "*"
from datetime import datetime
import logging
import tempfile
import json


# Internal imports
# -- Internal project Library imports


# External librabries
import pandas as pd
import requests
import yfinance as yf

from dagster import asset, AssetExecutionContext
from dagster import AssetSelection, define_asset_job


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Configurations:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #



# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Workflow definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

@asset
def get_stock_price(context: AssetExecutionContext) -> None:
    context.log.info("Getting Stock Price")
    tickers = [
                'AAPL', 
                'MSFT',
                'CRM',
                'NFLX'
            ]

    company_price_list = []
    for company in tickers:
        context.log.info("Currently processing: {0}".format(company))
        company_obj = yf.Ticker(company)
        df = company_obj.history(start='2024-03-01', end='2024-03-15')
        
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
    os.makedirs("data", exist_ok=True)
    result.to_csv('data/asset1.csv')

    context.log.info("Done")
    return


@asset(deps=[get_stock_price])
def eval_stock_agg(context: AssetExecutionContext) -> None:
    context.log.info("Aggregation of stock price")
    
    context.log.info("Reading data:")
    stock_price_df = pd.read_csv("data/asset1.csv")
    
    context.log.info("Creating Groups")
    temp_output = stock_price_df.groupby('ticker').agg({'Close': 'mean'})
    
    agg_output = temp_output.reset_index()
    
    context.log.info("Aggregated output: ")
    context.log.info(agg_output)
    
    context.log.info("Writing the data to asset 2:")
    agg_output.to_csv('data/asset2.csv')
    return


