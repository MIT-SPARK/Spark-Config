from setuptools import find_packages, setup

setup(
    name="spark_config",
    version="0.0.1",
    url="",
    author="Nathan Hughes",
    author_email="",
    description="Utility for managing Python YAML configuration",
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={"": ["*.yaml"]},
    install_requires=[],
)
