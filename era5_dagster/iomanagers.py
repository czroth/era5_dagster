import xarray as xr
from dagster import (
    DagsterInvariantViolationError,
    Field,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource,
    UPathIOManager,
)
from dagster import _check as check
from dagster import io_manager
from upath import UPath


class XArrayFSIOManager(UPathIOManager):

    extension: str = ".nc"

    def __init__(self, base_dir=None, **kwargs):
        self.base_dir = check.opt_str_param(base_dir, "base_dir")

        super().__init__(base_path=UPath(base_dir, **kwargs))

    def dump_to_path(self, context: OutputContext, obj: xr.Dataset, path: UPath):
        encoding = {var: {"zlib": True} for var in obj.data_vars}
        try:
            obj.to_netcdf(
                path,
                encoding=encoding,
            )
            context.add_output_metadata(
                {
                    "num_times": obj.sizes["time"],
                    "num_latitudes": obj.sizes["latitude"],
                    "num_longitudes": obj.sizes["longitude"],
                }
            )
        except (AttributeError, RecursionError, ImportError) as e:
            executor = context.step_context.pipeline_def.mode_definitions[
                0
            ].executor_defs[0]

            if isinstance(e, RecursionError):
                # if obj can't be pickled because of RecursionError then __str__() will also
                # throw a RecursionError
                obj_repr = f"{obj.__class__} exceeds recursion limit and"
            else:
                obj_repr = obj.__str__()

            raise DagsterInvariantViolationError(
                f"Object {obj_repr} is not picklable. You are currently using the "
                f"xarray_io_manager and the {executor.name}. You will need to use a different "
                "io manager to continue using this output. For example, you can use the "
                "mem_io_manager with the in_process_executor.\n"
                "For more information on io managers, visit "
                "https://docs.dagster.io/concepts/io-management/io-managers \n"
                "For more information on executors, vist "
                "https://docs.dagster.io/deployment/executors#overview"
            ) from e

    def load_from_path(self, context: InputContext, path: UPath) -> xr.Dataset:
        return xr.open_dataset(path)


@io_manager(
    config_schema={"base_dir": Field(StringSource, is_required=False)},
    description="Filesystem IO manager that stores and retrieves values using xarray.",
)
def xarray_fs_io_manager(init_context: InitResourceContext) -> XArrayFSIOManager:
    base_dir = init_context.resource_config.get(
        "base_dir", init_context.instance.storage_directory()  # type: ignore
    )
    return XArrayFSIOManager(base_dir=base_dir)
