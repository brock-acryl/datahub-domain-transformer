# DataHub Ingestion Transformers

This repository contains custom DataHub ingestion transformers that extend the functionality of DataHub's metadata ingestion pipeline. Each transformer has its own detailed documentation in its respective README file.

## Available Transformers

### 1. Domain Transformer
**File**: `domain_transformer.py`  
**Documentation**: [README_DOMAIN_TRANSFORMER.md](README_DOMAIN_TRANSFORMER.md)

Automatically maps datasets to domains based on CSV files or database tables. Creates missing domains automatically and assigns datasets to the appropriate domains during ingestion.

**Key Features:**
- Dual source support (CSV files or database tables)
- Automatic domain creation
- Multi-platform support (Teradata, Snowflake, PostgreSQL, etc.)
- Hierarchical domain structures with parent domains

### 2. Qualified Name Structured Properties Transformer
**File**: `qualified_name_structured_properties.py`  
**Documentation**: [README_QUALIFIED_NAME_STRUCTURED_PROPERTIES.md](README_QUALIFIED_NAME_STRUCTURED_PROPERTIES.md)

Automatically adds structured properties to entities based on qualified name parsing. Extracts components from entity URNs (database, table, column, etc.) and adds them as structured properties.

**Key Features:**
- Universal transformer (works on all entity types)
- Automatic qualified name parsing
- Configurable properties
- Safe merging with existing structured properties

## Installation

### Option 1: Install as a Package

```bash
pip install -e .
```

### Option 2: Copy Transformer Files

Copy the transformer Python files to your DataHub ingestion environment and reference them in your ingestion recipe.

## Requirements

- Python 3.7+
- `acryl-datahub[all]>=0.12.0` (DataHub Python SDK)
- `sqlalchemy>=1.4.0` (for database connections, required by Domain Transformer)
- `pyyaml>=6.0` (for configuration)

Install all requirements:

```bash
pip install -r requirements.txt
```

## Usage

Each transformer has its own configuration requirements. Please refer to the respective README files for detailed usage instructions:

- [Domain Transformer Documentation](README_DOMAIN_TRANSFORMER.md)
- [Qualified Name Structured Properties Transformer Documentation](README_QUALIFIED_NAME_STRUCTURED_PROPERTIES.md)

## General Usage Pattern

Transformers are used in DataHub ingestion recipes like this:

```yaml
transformers:
  - type: "transformer_module:TransformerClass"
    config:
      # Transformer-specific configuration
      # See individual README files for details
```

## Contributing

When adding a new transformer to this repository:

1. Create the transformer Python file
2. Create a corresponding `README_<TRANSFORMER_NAME>.md` file with detailed documentation
3. Update this main README.md to include the new transformer
4. Add any required dependencies to `requirements.txt`
5. Update `setup.py` if needed

## License

[Add your license information here]

## Support

For issues, questions, or contributions related to specific transformers, please refer to the individual transformer README files or open an issue in this repository.
