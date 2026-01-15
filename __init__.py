"""
DataHub Domain Transformer Package

A DataHub ingestion transformer for automatic domain mapping from CSV files or database tables.
"""

from .domain_transformer import DomainTransformer, DomainTransformerConfig

__all__ = ["DomainTransformer", "DomainTransformerConfig"]
__version__ = "1.0.0"
