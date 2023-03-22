import datetime as dt
from urllib.request import urlopen

import xarray as xr
from dagster import DailyPartitionsDefinition, asset

from era5_dagster.ecmwf_exceptions import ECMWFRequestException, ECMWFRetrieveException


era5_daily_partition_def = DailyPartitionsDefinition(
    start_date="2023-03-01",
    end_offset=-6,
)


@asset(
    required_resource_keys={"cds_api"},
    partitions_def=era5_daily_partition_def,
)
def era5_daily_temperature(context) -> xr.Dataset:
    """Retrieve daily ERA5 temperature data."""
    partition_date_str = context.asset_partition_key_for_output()
    date = dt.datetime.fromisoformat(partition_date_str)
    context.log.info(f"Retrieving data for {date}")
    try:
        result = context.resources.cds_api.retrieve(
            "reanalysis-era5-pressure-levels",
            {
                "variable": "temperature",
                "pressure_level": "1000",
                "product_type": "reanalysis",
                "year": f"{date.year}",
                "month": f"{date.month:02d}",
                "day": f"{date.day:02d}",
                "time": [f"{hour:02d}" for hour in range(24)],
                "format": "netcdf",
            },
        )
    except Exception as e:
        raise ECMWFRequestException(e)

    try:
        with urlopen(result.location) as f:
            ds = xr.open_dataset(f.read())
    except Exception as e:
        raise ECMWFRetrieveException(e)

    return ds
