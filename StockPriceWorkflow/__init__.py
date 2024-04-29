from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)

from . import assets

all_assets = load_assets_from_modules([assets])

# Addition: define a job that will materialize the assets
stock_agg_job = define_asset_job("stock_agg_job", selection=AssetSelection.all())

defs = Definitions(
    assets=all_assets,
    jobs=[stock_agg_job], 
)