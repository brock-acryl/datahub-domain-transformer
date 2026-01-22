# DataHub Qualified Name Structured Properties Transformer

A DataHub universal ingestion transformer that automatically adds structured properties to entities based on qualified name parsing. The transformer extracts components from entity URNs (database, table, column, etc.) and adds them as structured properties.

## Features

* **Universal Transformer**: Works on all entity types (datasets, containers, etc.)
* **Qualified Name Parsing**: Automatically extracts database, table, column, and other components from entity URNs
* **Configurable Properties**: Only adds structured properties that are explicitly configured
* **Flexible URN Format**: Supports multiple qualified name formats and URN structures
* **No Conflicts**: Safely merges with existing structured properties

## Installation

1. Copy the `qualified_name_structured_properties.py` file to your DataHub ingestion environment
2. Ensure you have the required dependencies:
   * `acryl-datahub[all]>=0.12.0` (DataHub Python SDK)
   * `pyyaml>=6.0` (for configuration)

Or install as a package:

```bash
pip install -e .
```

## Configuration

### Basic Configuration

```yaml
transformers:
  - type: "qualified_name_structured_properties:QualifiedNameStructuredProperties"
    config:
      environment: "prod"
      platform_prefix: "teradata.ndw.prod"
      structured_property_urns:
        qualified_name: "urn:li:structuredProperty:f90c7bc1-93d3-4b59-8d48-9d63ab9b92e2"
```

### Configuration Parameters

#### Required Parameters

* **structured_property_urns**: Dictionary mapping property names to their structured property URNs. Only properties listed here will be added to entities.

#### Optional Parameters

* **environment**: Environment identifier (e.g., "prod", "dev", "staging") - default: `"prod"`
* **platform_prefix**: Platform prefix for qualified names - default: `"teradata.ndw.prod"`

### Supported Property Names

The transformer can extract and add the following properties (only if configured in `structured_property_urns`):

* **qualified_name**: Full qualified name (e.g., `database.table@platform`)
* **environment**: Environment identifier
* **platform**: Platform name
* **entity_type**: Type of entity (application, database, table, column)
* **application_id**: Application identifier (for application entities)
* **database_name**: Database name (for database/table/column entities)
* **table_name**: Table name (for table/column entities)
* **column_name**: Column name (for column entities)

## Supported Qualified Name Formats

The transformer supports parsing the following qualified name formats:

### Application (Domain)
* Format: `<application_id>@<env>`
* Example: `6883@prod`
* Extracted properties: `application_id`, `environment`, `qualified_name`

### Database/Schema
* Format: `<database_name>@<platform>`
* Example: `ndw_base_pii_views@teradata.ndw.prod`
* Extracted properties: `database_name`, `platform`, `environment`, `qualified_name`

### Table/View
* Format: `<database_name>.<table_name>@<platform>`
* Example: `ndw_base_pii_views.ticket_task_user@teradata.ndw.prod`
* Extracted properties: `database_name`, `table_name`, `platform`, `environment`, `qualified_name`

### Column
* Format: `<database_name>.<table_name>.<column_name>@<platform>`
* Example: `ndw_base_pii_views.ticket_task_user.source_user_id@teradata.ndw.prod`
* Extracted properties: `database_name`, `table_name`, `column_name`, `platform`, `environment`, `qualified_name`

## Usage Examples

### Example 1: Basic Usage with Single Property

```yaml
source:
  type: "teradata"
  config:
    host_port: "teradata.example.com:1025"
    username: "user"
    password: "pass"

transformers:
  - type: "qualified_name_structured_properties:QualifiedNameStructuredProperties"
    config:
      environment: "prod"
      platform_prefix: "teradata.ndw.prod"
      structured_property_urns:
        qualified_name: "urn:li:structuredProperty:f90c7bc1-93d3-4b59-8d48-9d63ab9b92e2"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
    token: "your-token"
```

### Example 2: Multiple Properties

```yaml
transformers:
  - type: "qualified_name_structured_properties:QualifiedNameStructuredProperties"
    config:
      environment: "prod"
      platform_prefix: "teradata.ndw.prod"
      structured_property_urns:
        qualified_name: "urn:li:structuredProperty:qualified_name_urn"
        database_name: "urn:li:structuredProperty:database_name_urn"
        table_name: "urn:li:structuredProperty:table_name_urn"
        environment: "urn:li:structuredProperty:environment_urn"
        platform: "urn:li:structuredProperty:platform_urn"
```

### Example 3: Combined with Other Transformers

```yaml
transformers:
  - type: "domain_transformer:DomainTransformer"
    config:
      csv_file: "domain_mapping.csv"
      mapping:
        database: "DatabaseName"
        table: "TableName"
        domain: "DomainID"
      static:
        platform: "teradata"
  
  - type: "qualified_name_structured_properties:QualifiedNameStructuredProperties"
    config:
      environment: "prod"
      platform_prefix: "teradata.ndw.prod"
      structured_property_urns:
        qualified_name: "urn:li:structuredProperty:qualified_name_urn"
        database_name: "urn:li:structuredProperty:database_name_urn"
        table_name: "urn:li:structuredProperty:table_name_urn"
```

## How It Works

1. **URN Parsing**: When an entity is ingested, the transformer parses the entity URN to extract qualified name components
2. **Component Extraction**: Based on the URN format, it identifies:
   * Entity type (application, database, table, column)
   * Database name (if applicable)
   * Table name (if applicable)
   * Column name (if applicable)
   * Platform and environment
3. **Property Creation**: Creates structured property assignments for each configured property
4. **Aspect Merging**: Safely merges new properties with existing structured properties (avoids duplicates)

## URN Format Support

The transformer handles multiple URN formats:

* **Standard Format**: `urn:li:dataset:(urn:li:dataPlatform:teradata,database.table,PROD)`
* **With Qualified Name**: `urn:li:dataset:(urn:li:dataPlatform:teradata,database.table@platform,PROD)`
* **Direct Snapshot**: Works with both `MetadataChangeEventClass` and direct snapshot structures

## Important Notes

### Structured Property Definitions

**Before using this transformer, you must create the structured property definitions in DataHub.** The transformer will only add properties for URNs that exist in your DataHub instance.

To create structured properties:
1. Use the DataHub UI: Settings → Structured Properties → Create New
2. Or use the DataHub API/CLI
3. Note the URN of each structured property definition
4. Add the URNs to your `structured_property_urns` configuration

### Property Selection

**Only properties explicitly listed in `structured_property_urns` will be added.** If a property is not in the configuration, it will not be added to entities, even if the transformer can extract the value.

### Execution Order

When using multiple transformers, they execute in the order listed. The qualified name structured properties transformer can be placed anywhere in the list, but typically works well:
- **After domain transformers**: To add properties to entities that have been assigned to domains
- **Before other transformers**: If other transformers need the structured properties

## Troubleshooting

### Properties Not Being Added

* **Check structured property definitions**: Ensure all URNs in `structured_property_urns` exist in DataHub
* **Verify configuration**: Check that property names match exactly (case-sensitive)
* **Check logs**: Look for parsing errors or skipped entities in the ingestion logs

### Parsing Errors

* **URN format**: Verify your entity URNs match the expected formats
* **Platform prefix**: Ensure `platform_prefix` matches your actual platform naming
* **Environment**: Check that the environment in URNs matches your `environment` config

### Validation Errors

* **Missing definitions**: If you see "Unexpected null value found for structured property definition" errors, the structured property definition doesn't exist in DataHub
* **Create definitions first**: Create all required structured property definitions before running ingestion

### Import Errors

* **Module not found**: Ensure the transformer file is in your Python path
* **Install package**: If using as a package, run `pip install -e .` from the repository root
* **Check import path**: Verify the `type` in your config matches your file structure

## Development

### File Structure

```
.
├── qualified_name_structured_properties.py  # Transformer implementation
├── setup.py                                  # Package setup (if using as package)
├── __init__.py                               # Package exports
└── README.md                                 # This file
```

### Testing

Test the transformer locally:

```bash
# Validate configuration
datahub ingest -c config.yml --dry-run

# Run ingestion
datahub ingest -c config.yml
```

## License

This transformer is provided as-is for use with DataHub.

## Related

* [DataHub Documentation](https://datahubproject.io/docs/)
* [DataHub Structured Properties](https://datahubproject.io/docs/metadata/structured-properties/)
* [DataHub Transformers](https://datahubproject.io/docs/metadata-ingestion/transformers/)
