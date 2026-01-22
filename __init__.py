"""
DataHub Domain Transformer Package

A DataHub ingestion transformer for automatic domain mapping from CSV files or database tables.
"""

from .domain_transformer import DomainTransformer, DomainTransformerConfig
from .qualified_name_structured_properties import QualifiedNameStructuredProperties

__all__ = ["DomainTransformer", "DomainTransformerConfig", "QualifiedNameStructuredProperties"]
__version__ = "1.0.0"
