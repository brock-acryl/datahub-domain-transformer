"""
Setup file for DataHub Domain Transformer
"""

from setuptools import setup, find_packages

setup(
    name="datahub-domain-transformer",
    version="1.0.0",
    description="DataHub ingestion transformer for automatic domain mapping",
    author="Your Name",
    author_email="your.email@example.com",
    py_modules=[
        "domain_transformer",
        "qualified_name_structured_properties",
        "simple_add_structured_properties",
    ],
    install_requires=[
        "datahub>=0.8.0",
        "sqlalchemy>=1.4.0",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
