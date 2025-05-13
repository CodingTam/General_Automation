from setuptools import setup, find_packages

setup(
    name="automation_framework",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "PyYAML",
        # Add other dependencies from requirements.txt
    ],
    python_requires=">=3.6",
) 