from dagster import (
    Definitions,
    build_schedule_from_partitioned_job,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets
from .iomanagers import xarray_fs_io_manager
from .resources import cds_api_resource

resource_definitions = {
    "cds_api": cds_api_resource,
    "xarray_manager": xarray_fs_io_manager,
}


default_config = {
    "resources": {
        "cds_api": {
            "config": {
                "key": "your-key-here",
                "url": "https://cds.climate.copernicus.eu/api/v2",
            }
        }
    }
}


era5_daily_asset_job = define_asset_job(
    "era5_daily_job",
    selection=[assets.era5_daily_temperature, assets.era5_daily_mean_temperature],
    config=default_config,
    partitions_def=assets.era5_daily_partition_def,
)


era5_schedule = build_schedule_from_partitioned_job(
    era5_daily_asset_job,
)


defs = Definitions(
    assets=load_assets_from_modules(
        [assets],
        group_name="era5",
    ),
    resources=resource_definitions,
    schedules=[era5_schedule],
)
