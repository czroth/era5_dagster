from setuptools import find_packages, setup

setup(
    name="era5_dagster",
    packages=find_packages(exclude=["era5_dagster_tests"]),
    install_requires=[
        "cdsapi",
        "dagster",
        "dask",
        "dagster-cloud",
        "xarray[io]",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
