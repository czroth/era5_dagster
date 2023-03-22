import cdsapi
from dagster import Field, String, resource


@resource(
    config_schema={
        "url": Field(String),
        "key": Field(String),
    },
)
def cds_api_resource(context):
    """CDS (Climate Data Source) resource"""
    context.log.info("Creating CDS API client")
    return cdsapi.Client(
        url=context.resource_config["url"],
        key=context.resource_config["key"],
        info_callback=context.log.info,
        warning_callback=context.log.warning,
        error_callback=context.log.error,
        debug_callback=context.log.debug,
    )
