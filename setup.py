from setuptools import find_packages, setup

setup(
    name="DagsterWorkflow",
    packages=find_packages(exclude=["DagsterWorkflow_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
