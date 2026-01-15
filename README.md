# DataHub Domain Transformer

A DataHub ingestion transformer that automatically maps datasets to domains based on CSV files or database tables. The transformer creates missing domains automatically and assigns datasets to the appropriate domains during ingestion.

## Features

- **Dual Source Support**: Use either CSV files or database tables as the mapping source
- **Automatic Domain Creation**: Creates missing domains in DataHub automatically
- **Connection Reuse**: Reuses source connection from the ingestion recipe (for table-based lookups)
- **Multi-Platform Support**: Works with Teradata, Snowflake, PostgreSQL, and other SQL databases
- **Flexible Matching**: Supports matching by platform, database, schema, and table with fallback logic
- **Parent Domain Support**: Supports hierarchical domain structures with parent domains

## Installation

1. Copy the `domain_transformer.py` file to your DataHub ingestion environment
2. Ensure you have the required dependencies:
   - `datahub` (DataHub Python SDK)
   - `sqlalchemy` (for database connections)

## Configuration

### CSV-Based Mapping

Use a CSV file to define the mapping between datasets and domains:

```yaml
transformers:
  - type: "domain_transformer:DomainTransformer"
    config:
      csv_file: "domain_mapping.csv"
      # datahub_url and datahub_token are optional - auto-detected from sink config
      # datahub_url: "http://localhost:8080"  # Optional: only needed if different from sink
      # datahub_token: "your_token"  # Optional: only needed if different from sink
      mapping:
        database: "DatabaseName"
        schema: null
        table: "TableName"
        domain: "DomainID"
        parent_domain: null
        domain_description: null
      static:
        platform: "teradata"
        domain_parent: "parent_domain_id"  # Optional
      semantics: "PATCH"
```

### Database Table-Based Mapping

Use a database table in your source system to define the mapping:

```yaml
transformers:
  - type: "domain_transformer:DomainTransformer"
    config:
      table_name: "domain_mapping_table"
      database: "your_database"  # Optional
      schema: null  # Optional
      # datahub_url and datahub_token are optional - auto-detected from sink config
      # datahub_url: "http://localhost:8080"  # Optional: only needed if different from sink
      # datahub_token: "your_token"  # Optional: only needed if different from sink
      mapping:
        database: "DatabaseName"
        schema: null
        table: "TableName"
        domain: "DomainID"
        parent_domain: null
        domain_description: null
      static:
        platform: "teradata"
        domain_parent: "parent_domain_id"  # Optional
      semantics: "PATCH"
```

## CSV Format

The CSV file should have columns for:
- **DatabaseName**: Database name (optional, can be null)
- **TableName**: Table name (required)
- **DomainID**: Domain identifier (required)
- **ParentDomain**: Parent domain ID (optional)
- **DomainDescription**: Domain description (optional)

Example CSV:

```csv
DatabaseName,TableName,DomainID,TableKind
analytics_db,customer_dimension,analytics_001,V
analytics_db,sales_fact,analytics_001,V
marketing_db,campaign_data,marketing_001,V
```

## Database Table Format

The database table should have columns matching the `mapping` configuration. For example, if your mapping specifies:
- `database: "DatabaseName"`
- `table: "TableName"`
- `domain: "DomainID"`

Then your table should have columns named `DatabaseName`, `TableName`, and `DomainID`.

## Configuration Parameters

### Required Parameters

- **csv_file** OR **table_name**: Path to CSV file or name of database table
- **mapping**: Dictionary mapping CSV/table columns to dataset components
  - **table**: Column name for table name (required)
  - **domain**: Column name for domain ID (required)
  - **database**: Column name for database name (optional)
  - **schema**: Column name for schema name (optional)
  - **parent_domain**: Column name for parent domain (optional)
  - **domain_description**: Column name for domain description (optional)
- **static**: Static configuration values
  - **platform**: Platform name (required, must match source type)

### Optional Parameters

- **database**: Database name for table-based lookups (uses source default if not provided)
- **schema**: Schema name for table-based lookups (optional)
- **datahub_url**: (Deprecated) No longer needed - uses PipelineContext.graph instead
- **datahub_token**: (Deprecated) No longer needed - uses PipelineContext.graph instead
- **static.domain_parent**: Default parent domain for all domains
- **semantics**: "PATCH" (add to existing) or "REPLACE" (replace all)

### DataHub Connection

The transformer uses `PipelineContext.graph` which is automatically configured with your sink's server URL and authentication token. **No need to configure `datahub_url` or `datahub_token` separately!** 

The `PipelineContext.graph` object is already set up with the connection information from your sink configuration, so domain creation will automatically use the same authentication as the rest of your ingestion pipeline.

## How It Works

1. **Initialization**: The transformer loads the mapping from CSV or database table
2. **URN Parsing**: When a dataset is ingested, the transformer parses the dataset URN to extract platform, database, schema, and table components
3. **Matching**: The transformer matches the parsed components against the mapping using fallback logic:
   - First tries exact match: (platform, database, schema, table)
   - Falls back to: (platform, database, None, table)
   - Falls back to: (platform, None, schema, table)
   - Falls back to: (platform, None, None, table)
4. **Domain Creation**: If a match is found, the transformer checks if the domain exists in DataHub and creates it if missing
5. **Domain Assignment**: The transformer adds the domain to the dataset's domains aspect

## Supported Platforms

The transformer supports reading connection information from the source config for:
- **Teradata**: Uses `host_port`, `username`, `password`
- **Snowflake**: Uses `account`, `username`, `password`, `database`, `warehouse`, `schema`
- **PostgreSQL**: Uses `host`, `port`, `username`, `password`, `database`
- **Generic**: Falls back to `sql_alchemy_url` if available

## Examples

### Example 1: CSV-Based Mapping with Teradata

```yaml
source:
  type: "teradata"
  config:
    host_port: "teradata.example.com:1025"
    username: "user"
    password: "pass"
    database_pattern:
      allow:
        - "analytics_db"

transformers:
  - type: "domain_transformer:DomainTransformer"
    config:
      csv_file: "domain_mapping.csv"
      # datahub_url and datahub_token auto-detected from sink config
      mapping:
        database: "DatabaseName"
        table: "TableName"
        domain: "DomainID"
      static:
        platform: "teradata"
      semantics: "PATCH"
```

### Example 2: Table-Based Mapping with Snowflake

```yaml
source:
  type: "snowflake"
  config:
    account: "your_account"
    username: "user"
    password: "pass"
    database: "analytics_db"

transformers:
  - type: "domain_transformer:DomainTransformer"
    config:
      table_name: "domain_mapping"
      database: "analytics_db"
      schema: "metadata"
      # datahub_url and datahub_token auto-detected from sink config
      mapping:
        database: "DatabaseName"
        schema: "SchemaName"
        table: "TableName"
        domain: "DomainID"
      static:
        platform: "snowflake"
      semantics: "PATCH"
```

## Troubleshooting

### Domain Not Created

- Ensure `datahub_url` is provided in the transformer config
- Check that the DataHub token (if required) is valid
- Verify the domain ID format is correct

### Mapping Not Found

- Verify the CSV/table column names match the `mapping` configuration
- Check that the platform name in `static.platform` matches the source type
- Ensure the dataset URN format matches expected patterns

### Connection Errors (Table-Based)

- Verify the source connection details are correct
- Ensure the table exists and is accessible
- Check that column names in the table match the `mapping` configuration

## License

This transformer is provided as-is for use with DataHub.
