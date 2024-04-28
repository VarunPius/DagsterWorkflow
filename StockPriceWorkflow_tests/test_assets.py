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

from dagster import asset


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Workflow definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

def get_stock_price() -> None:
    tickers = [
                'AAPL', 
                'MSFT',
                #'CRM'
            ]

    company_price_list = []
    for company in tickers:
        print("Currently processing: {0}".format(company))
        company_obj = yf.Ticker(company)
        df = company_obj.history(start='2024-03-01', end='2024-03-15')
        df['ticker'] = company
        # df['date'] = df.index
        df = df.reset_index()
        print(df)
        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
        print(df)
        print("Done fetching data for: {0}".format(company))
        print("Dataframe schema: {0}".format(df.columns))
        df = df[['ticker', 'date', 'Close']]
 
        company_price_list.append(df)
    
    print("Done fetching all data")
    result = pd.concat(company_price_list)
    result = result.reset_index()
    print("Result:")
    print("Result schema: {0}".format(result.columns))
    print(result)
    result.drop(['index'], axis=1)
    print(result)
    logging.info("Done")
    return result


def eval_stock_agg(df):
    print("Evaluate")
    op = df.groupby('ticker').agg({'Close': 'mean'})
    op = op.reset_index()
    print(op)
    return


df = get_stock_price()
eval_stock_agg(df)
