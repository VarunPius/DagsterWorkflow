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
#   v5.0.0   | StockPriceWorkflow: Adding Directory structures                                                                                       #
#   v5.0.1   | StockPriceWorkflow: Cleaning directory structures and package mgmt                                                                    #
# -------------------------------------------------------------------------------------------------------------------------------------------------- #


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Library Imports:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# System Libraries
# Internal imports
from . import assets as assets
#from .assets import stock_price_assets     # won't be necessary as whole package is imported in preceding line
                                            # This will just import one module
                                            # will get lengthy when too many modules under `assets` package


# External librabries
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    load_assets_from_package_module
)


# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Workflow definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #

# Asset definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
assets_all = load_assets_from_package_module(assets)
assets_stock_price = load_assets_from_modules([assets.stock_price_assets])
assets_stock_price_alt = [assets.stock_price_assets.get_stock_price, assets.stock_price_assets.eval_stock_agg]
'''
Preferably, create `assets_all` and should be all. One line will import all assets from `assets` module.
Then in the jobs, define what assets you need either with the help of `groups` or just pass a list for `selection`
Check how imports are done.
We either import file names for which you use -> load_assets_from_modules
If you import the whole package (here, `assets` directory) instead of just a module(file) from the packages, use -> load_assets_from_package_module

Note: load_assets_from_package_module returns an iterable list.
    Sometimes you are returning assets from other sources such as dbt using load_assets_from_dbt_cloud_job.
    Now, load_assets_from_dbt_cloud_job doesn't return a iterable, but CacheableAssetsDefinition
    In this scenario you add this to the iterable list because you definitions either needs an iterable or a single Asset Definition
    so do something like this:
    dbt_cloud_assets = load_assets_from_dbt_cloud_job(
        dbt_cloud=dbt_cloud_instance, job_id = job_id)
    assets_all = load_assets_from_package_module(assets)
    assets_all.append(dbt_cloud_assets)
    Here, we are simply adding dbt_cloud_assets to the assets_all iterable
'''


# Job definitions:
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# Define a job that will materialize the assets
job_all = define_asset_job("job_all", selection=AssetSelection.all())
job_all_alt = define_asset_job("job_all", selection=assets_all)
job_stock_agg = define_asset_job("job_stock_agg", selection=AssetSelection.groups("grp_stock_agg"))
job_stock_price_hist = define_asset_job("job_stock_hist", selection=[assets.stock_price_assets.get_stock_price])


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
