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
#   v0.0.0   | StockPriceWorkflow: Setting up Dagster project                                                                                        #
#   v0.0.1   | StockPriceWorkflow: Adding comments and description                                                                                   #
#   v1.0.0   | StockPriceWorkflow: Getting Stock price from sample tickers                                                                           #
#   v2.0.0   | StockPriceWorkflow: Calculating stock price aggregates                                                                                #
#   v3.0.0   | StockPriceWorkflow: Creating jobs                                                                                                     #
#   v4.0.0   | StockPriceWorkflow: Changing assets to pass dataframe between assets                                                                  #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
# Internal imports
from .assets import stock_price_assets


# External librabries
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Workflow definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

assets_all = load_assets_from_modules([stock_price_assets])
assets_stock_price = [stock_price_assets.get_stock_price, stock_price_assets.eval_stock_agg]

# Addition: define a job that will materialize the assets
job_all = define_asset_job("job_all", selection=AssetSelection.all())
job_all_alt = define_asset_job("job_all", selection=assets_all)
job_stock_agg = define_asset_job("job_stock_agg", selection=AssetSelection.groups("grp_stock_agg"))
job_stock_price_hist = define_asset_job("job_stock_hist", selection=[stock_price_assets.get_stock_price])


defs = Definitions(
    #assets = [stock_price_assets.get_stock_price, stock_price_assets.eval_stock_agg],
    assets = assets_all,
    jobs =  [
        job_all,
        job_stock_agg,
        job_stock_price_hist,
    ],
    schedules = [
        ScheduleDefinition(
            name = "schedule_stock_price_agg",
            job_name = "job_stock_agg",
            cron_schedule = "0 8 * * *",
        ),
        ScheduleDefinition(
            name = "schedule_stock_price_hist",
            job_name = "job_stock_hist",
            cron_schedule = "0 8 * * *",
        ),
    ],
)
