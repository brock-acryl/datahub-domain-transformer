"""
DataHub Domain Transformer Package

A DataHub ingestion transformer for automatic domain mapping from CSV files or database tables.
"""

from .domain_transformer import DomainTransformer, DomainTransformerConfig
from .qualified_name_structured_properties import QualifiedNameStructuredProperties
from .simple_add_structured_properties import (
    SimpleAddStructuredProperties,
    SimpleAddStructuredPropertiesConfig,
)

__all__ = [
    "DomainTransformer",
    "DomainTransformerConfig",
    "QualifiedNameStructuredProperties",
    "SimpleAddStructuredProperties",
    "SimpleAddStructuredPropertiesConfig",
]
__version__ = "1.0.0"
