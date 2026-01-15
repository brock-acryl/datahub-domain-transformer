"""
Domain Transformer for DataHub Ingestion

This ingestion transformer automatically maps datasets to domains based on CSV files or database tables.
It follows the BaseTransformer pattern and creates missing domains automatically.

Features:
- Supports CSV file or database table as the mapping source
- Automatically creates missing domains in DataHub
- Reuses source connection from the ingestion recipe
- Supports multiple database platforms (Teradata, Snowflake, PostgreSQL, etc.)
"""

import csv
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import DomainsClass, DomainPropertiesClass
from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

logger = logging.getLogger(__name__)


@dataclass
class DomainInfo:
    """Information about a domain from CSV or database table"""
    domain_id: str
    domain_name: Optional[str]
    parent_domain: Optional[str]
    domain_description: Optional[str]


class DomainTransformerConfig(ConfigModel):
    """Configuration for Domain Transformer"""
    csv_file: Optional[str] = None
    table_name: Optional[str] = None  # Database table name for lookup
    database: Optional[str] = None  # Database name (optional, uses source default if not provided)
    schema: Optional[str] = None  # Schema name (optional)
    datahub_url: Optional[str] = None  # Optional: DataHub GMS URL (auto-detected from sink config if not provided)
    datahub_token: Optional[str] = None  # Optional: DataHub auth token (auto-detected from sink config if not provided)
    mapping: Dict[str, Optional[str]] = {}
    static: Dict[str, Optional[str]] = {}
    semantics: str = "PATCH"  # PATCH = add to existing, REPLACE = replace existing


class DomainTransformer(BaseTransformer, SingleAspectTransformer):
    """Lookup domains from a CSV file or database table and create missing domains"""

    ctx: PipelineContext
    config: DomainTransformerConfig

    def __init__(self, config: DomainTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.created_domains = set()
        self._mapping_loaded = False
        self.domain_mapping = {}
        self.domain_info_map = {}
        
        # Initialize emitter for domain creation
        # Try to get DataHub URL and token from pipeline context (sink config) if not provided
        datahub_url = config.datahub_url
        datahub_token = config.datahub_token
        
        if not datahub_url:
            # Try to get from pipeline context (sink configuration)
            datahub_url, datahub_token = self._get_datahub_connection_from_context()
        
        self.emitter = None
        if datahub_url:
            self.emitter = DatahubRestEmitter(
                gms_server=datahub_url,
                token=datahub_token
            )
            logger.info(f"Initialized emitter for domain creation: {datahub_url}")
        else:
            logger.warning("No datahub_url provided and could not retrieve from pipeline context - domain creation will be limited to graph client")
        
        # Load CSV immediately if provided (doesn't need source connection)
        if config.csv_file:
            self.domain_mapping, self.domain_info_map = self._load_mapping_from_csv(
                config.csv_file,
                config.mapping,
                config.static
            )
            self._mapping_loaded = True
        elif not config.table_name:
            raise ValueError("Either csv_file or table_name must be provided in transformer config")
        # Table loading will be done lazily when first needed (after source is initialized)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DomainTransformer":
        """
        Create transformer instance from config dict
        
        Args:
            config_dict: Configuration dictionary
            ctx: Pipeline context
            
        Returns:
            DomainTransformer instance
        """
        config = DomainTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        """Return entity types this transformer processes"""
        return ["dataset"]

    def aspect_name(self) -> str:
        """Return the aspect name this transformer modifies"""
        return "domains"

    def _load_mapping_from_csv(
        self,
        csv_file: str,
        mapping_config: dict,
        static_config: dict
    ) -> Tuple[Dict[Tuple[Optional[str], Optional[str], Optional[str], str], str], Dict[str, DomainInfo]]:
        """
        Load mapping from CSV file

        Args:
            csv_file: Path to CSV file
            mapping_config: Column mapping configuration
            static_config: Static values (platform, env, etc.)

        Returns:
            Tuple of:
            - Dictionary keyed by (platform, database, schema, table) tuple, value is domain_id
            - Dictionary keyed by domain_id, value is DomainInfo
        """
        mapping = {}
        domain_info_map = {}
        csv_path = Path(csv_file)

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        # Get static values
        platform = static_config.get("platform")
        if not platform:
            raise ValueError("Platform must be provided as static value in config")

        # Get column names
        database_column = mapping_config.get("database")
        schema_column = mapping_config.get("schema")
        table_column = mapping_config.get("table", "table")
        domain_column = mapping_config.get("domain", "domain")
        parent_domain_column = mapping_config.get("parent_domain")
        domain_description_column = mapping_config.get("domain_description")
        domain_parent = static_config.get("domain_parent")

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            for idx, row in enumerate(reader, start=1):
                try:
                    # Get table name (required)
                    table = row.get(table_column)
                    if not table or not table.strip():
                        logger.warning(f"Row {idx}: Skipping row with empty table name")
                        continue

                    # Get domain ID (required)
                    domain_id = row.get(domain_column)
                    if not domain_id or not domain_id.strip():
                        logger.warning(f"Row {idx}: Skipping row with empty domain ID")
                        continue

                    # Get optional fields
                    database = row.get(database_column) if database_column else None
                    schema = row.get(schema_column) if schema_column else None
                    parent_domain = row.get(parent_domain_column) if parent_domain_column else None
                    domain_description = row.get(domain_description_column) if domain_description_column else None

                    # Use static parent domain if column not provided
                    if not parent_domain:
                        parent_domain = domain_parent

                    # Normalize empty strings to None
                    database = database.strip() if database and database.strip() else None
                    schema = schema.strip() if schema and schema.strip() else None
                    table = table.strip()
                    domain_id = domain_id.strip()
                    parent_domain = parent_domain.strip() if parent_domain and parent_domain.strip() else None
                    domain_description = domain_description.strip() if domain_description and domain_description.strip() else None

                    # Create cache key: (platform, database, schema, table)
                    cache_key = (platform, database, schema, table)
                    mapping[cache_key] = domain_id

                    # Store domain info if not already stored
                    if domain_id not in domain_info_map:
                        domain_info_map[domain_id] = DomainInfo(
                            domain_id=domain_id,
                            domain_name=domain_id,  # Use domain_id as name if not provided
                            parent_domain=parent_domain,
                            domain_description=domain_description
                        )

                except KeyError as e:
                    logger.error(f"Row {idx}: Missing required column: {e}")
                except Exception as e:
                    logger.error(f"Row {idx}: Error processing row: {e}")

        logger.info(f"Loaded {len(mapping)} domain mappings from CSV file: {csv_file}")
        logger.info(f"Found {len(domain_info_map)} unique domains")
        return mapping, domain_info_map

    def _get_datahub_connection_from_context(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get DataHub URL and token from pipeline context (sink configuration)
        
        Returns:
            Tuple of (datahub_url, datahub_token) or (None, None) if not found
        """
        # Try to get from sink configuration
        if hasattr(self.ctx, 'pipeline_config'):
            pipeline_config = self.ctx.pipeline_config
            if hasattr(pipeline_config, 'sink'):
                sink = pipeline_config.sink
                # Check if it's a datahub-rest sink
                if hasattr(sink, 'config'):
                    sink_config = sink.config
                    # Try to get GMS server URL
                    gms_server = None
                    token = None
                    
                    if hasattr(sink_config, 'server'):
                        gms_server = sink_config.server
                    elif hasattr(sink_config, 'gms_server'):
                        gms_server = sink_config.gms_server
                    elif hasattr(sink_config, 'datahub_server'):
                        gms_server = sink_config.datahub_server
                    
                    # Try to get token
                    if hasattr(sink_config, 'token'):
                        token = sink_config.token
                    elif hasattr(sink_config, 'datahub_token'):
                        token = sink_config.datahub_token
                    
                    if gms_server:
                        logger.info(f"Retrieved DataHub URL from sink config: {gms_server}")
                        return (gms_server, token)
        
        # Try to get from environment variables (common DataHub pattern)
        import os
        gms_server = os.getenv('DATAHUB_GMS_URL') or os.getenv('DATAHUB_SERVER')
        token = os.getenv('DATAHUB_TOKEN')
        
        if gms_server:
            logger.info(f"Retrieved DataHub URL from environment: {gms_server}")
            return (gms_server, token)
        
        return (None, None)

    def _get_source_connection(self):
        """
        Get database connection by reading source config from pipeline context
        
        Returns:
            SQLAlchemy connection or engine
        """
        from sqlalchemy import create_engine
        from urllib.parse import quote_plus
        
        # Get source config from pipeline context
        source_config = None
        source_type = None
        
        # Try to get source config from pipeline_config
        if hasattr(self.ctx, 'pipeline_config'):
            pipeline_config = self.ctx.pipeline_config
            if hasattr(pipeline_config, 'source'):
                source_obj = pipeline_config.source
                # Try to get config as attribute
                if hasattr(source_obj, 'config'):
                    source_config = source_obj.config
                # Try to get type
                source_type = getattr(source_obj, 'type', None) or getattr(source_obj, '_type', None) or getattr(source_obj, 'source_type', None)
                # If source_obj itself looks like a config, use it
                if source_config is None and hasattr(source_obj, '__dict__'):
                    # Check if it has source config attributes
                    if any(attr in source_obj.__dict__ for attr in ['host_port', 'host', 'username', 'password']):
                        source_config = source_obj
        
        # If not found, try to get from context directly
        if source_config is None and hasattr(self.ctx, 'source'):
            ctx_source = self.ctx.source
            if hasattr(ctx_source, 'config'):
                source_config = ctx_source.config
            elif hasattr(ctx_source, '__dict__'):
                # Might be a config object itself
                source_config = ctx_source
            if not source_type:
                source_type = getattr(ctx_source, 'type', None) or getattr(ctx_source, '_type', None) or getattr(ctx_source, 'source_type', None)
        
        if source_config is None:
            raise ValueError("Source config not available in pipeline context. Cannot load mapping from database table.")
        
        # Extract connection parameters from source config
        # Handle both dict-like and object-like configs
        if hasattr(source_config, '__dict__'):
            config_dict = {k: v for k, v in source_config.__dict__.items() if not k.startswith('_')}
        elif isinstance(source_config, dict):
            config_dict = source_config
        else:
            # Try to access as attributes
            config_dict = {}
            for attr in ['host_port', 'host', 'port', 'username', 'password', 'database', 'schema', 'options']:
                if hasattr(source_config, attr):
                    config_dict[attr] = getattr(source_config, attr)
        
        # Determine source type
        if not source_type:
            source_type = config_dict.get('type') or config_dict.get('_type') or 'teradata'
        
        logger.info(f"Creating connection from source config (type: {source_type})")
        
        # Extract options/connect_args if present
        options = config_dict.get('options', {})
        connect_args = options.get('connect_args', {}) if isinstance(options, dict) else {}
        
        # Build connection string based on source type
        if source_type.lower() == 'teradata':
            # Teradata connection: teradatasql://user:pass@host:port/
            host_port = config_dict.get('host_port') or f"{config_dict.get('host', 'localhost')}:{config_dict.get('port', '1025')}"
            username = config_dict.get('username', '')
            password = config_dict.get('password', '')
            
            if not host_port:
                raise ValueError("host_port is required in Teradata source config")
            if not username:
                raise ValueError("username is required in Teradata source config")
            if not password:
                raise ValueError("password is required in Teradata source config")
            
            # URL encode username and password
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            
            connection_url = f"teradatasql://{username_encoded}:{password_encoded}@{host_port}/"
            logger.debug(f"Created Teradata connection URL: teradatasql://{username_encoded}:***@{host_port}/")
            
            # Create engine with connect_args if provided
            if connect_args:
                return create_engine(connection_url, connect_args=connect_args)
            
        elif source_type.lower() in ['snowflake', 'snowflake_legacy']:
            # Snowflake connection
            account = config_dict.get('account') or config_dict.get('account_id', '')
            username = config_dict.get('username') or config_dict.get('user', '')
            password = config_dict.get('password', '')
            database = config_dict.get('database', '')
            warehouse = config_dict.get('warehouse', '')
            schema = config_dict.get('schema', '')
            
            if not account or not username or not password:
                raise ValueError("account, username, and password are required in Snowflake source config")
            
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            
            # Build Snowflake connection string
            connection_url = f"snowflake://{username_encoded}:{password_encoded}@{account}/"
            if database:
                connection_url += f"{database}/"
            if schema:
                connection_url += f"{schema}"
            if warehouse:
                connection_url += f"?warehouse={warehouse}"
                
        elif source_type.lower() in ['postgres', 'postgresql']:
            # PostgreSQL connection
            host = config_dict.get('host', 'localhost')
            port = config_dict.get('port', '5432')
            username = config_dict.get('username', '')
            password = config_dict.get('password', '')
            database = config_dict.get('database', '')
            
            if not username or not password or not database:
                raise ValueError("username, password, and database are required in PostgreSQL source config")
            
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            
            connection_url = f"postgresql://{username_encoded}:{password_encoded}@{host}:{port}/{database}"
            
        else:
            # Generic SQLAlchemy connection - try to get sql_alchemy_url if available
            if 'sql_alchemy_url' in config_dict or hasattr(source_config, 'sql_alchemy_url'):
                connection_url = config_dict.get('sql_alchemy_url') or getattr(source_config, 'sql_alchemy_url', None)
                if connection_url:
                    if connect_args:
                        return create_engine(connection_url, connect_args=connect_args)
                    return create_engine(connection_url)
            raise ValueError(f"Unsupported source type: {source_type}. Supported types: teradata, snowflake, postgresql")
        
        # Create and return engine with connect_args if provided
        if connect_args:
            return create_engine(connection_url, connect_args=connect_args)
        return create_engine(connection_url)

    def _load_mapping_from_table(
        self,
        table_name: str,
        database: Optional[str],
        schema: Optional[str],
        mapping_config: dict,
        static_config: dict
    ) -> Tuple[Dict[Tuple[Optional[str], Optional[str], Optional[str], str], str], Dict[str, DomainInfo]]:
        """
        Load mapping from database table using source connection

        Args:
            table_name: Name of the mapping table
            database: Database name (optional, uses source default if not provided)
            schema: Schema name (optional)
            mapping_config: Column mapping configuration
            static_config: Static values (platform, env, etc.)

        Returns:
            Tuple of:
            - Dictionary keyed by (platform, database, schema, table) tuple, value is domain_id
            - Dictionary keyed by domain_id, value is DomainInfo
        """
        mapping = {}
        domain_info_map = {}
        
        # Get static values
        platform = static_config.get("platform")
        if not platform:
            raise ValueError("Platform must be provided as static value in config")

        # Get column names
        database_column = mapping_config.get("database")
        schema_column = mapping_config.get("schema")
        table_column = mapping_config.get("table", "table")
        domain_column = mapping_config.get("domain", "domain")
        parent_domain_column = mapping_config.get("parent_domain")
        domain_description_column = mapping_config.get("domain_description")
        domain_parent = static_config.get("domain_parent")

        # Get source connection
        connection = self._get_source_connection()
        
        # Build table reference
        # Import SQLAlchemy components
        from sqlalchemy import text
        
        # Construct full table name with database/schema if provided
        if database and schema:
            full_table_name = f"{database}.{schema}.{table_name}"
        elif database:
            full_table_name = f"{database}.{table_name}"
        elif schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name

        logger.info(f"Loading domain mapping from table: {full_table_name}")

        try:
            # Build SELECT query with all needed columns
            columns = [table_column, domain_column]
            if database_column:
                columns.append(database_column)
            if schema_column:
                columns.append(schema_column)
            if parent_domain_column:
                columns.append(parent_domain_column)
            if domain_description_column:
                columns.append(domain_description_column)

            # Construct SQL query
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {full_table_name}"

            # Execute query
            # Handle different connection types (engine, connection, etc.)
            if hasattr(connection, 'connect'):
                # It's an engine, get a connection
                with connection.connect() as conn:
                    result = conn.execute(text(query))
                    rows = result.fetchall()
            elif hasattr(connection, 'execute'):
                # It's a connection
                result = connection.execute(text(query))
                rows = result.fetchall()
            else:
                raise ValueError(f"Connection object {type(connection)} does not support execute() or connect()")

            # Process rows
            column_names = [col.strip() for col in columns]
            
            for idx, row in enumerate(rows, start=1):
                try:
                    # Convert row to dict
                    row_dict = dict(zip(column_names, row))

                    # Get table name (required)
                    table = row_dict.get(table_column)
                    if not table or (isinstance(table, str) and not table.strip()):
                        logger.warning(f"Row {idx}: Skipping row with empty table name")
                        continue

                    # Get domain ID (required)
                    domain_id = row_dict.get(domain_column)
                    if not domain_id or (isinstance(domain_id, str) and not domain_id.strip()):
                        logger.warning(f"Row {idx}: Skipping row with empty domain ID")
                        continue

                    # Get optional fields
                    db_value = row_dict.get(database_column) if database_column else None
                    schema_value = row_dict.get(schema_column) if schema_column else None
                    parent_domain = row_dict.get(parent_domain_column) if parent_domain_column else None
                    domain_description = row_dict.get(domain_description_column) if domain_description_column else None

                    # Use static parent domain if column not provided
                    if not parent_domain:
                        parent_domain = domain_parent

                    # Normalize values
                    database = str(db_value).strip() if db_value and str(db_value).strip() else None
                    schema = str(schema_value).strip() if schema_value and str(schema_value).strip() else None
                    table = str(table).strip()
                    domain_id = str(domain_id).strip()
                    parent_domain = str(parent_domain).strip() if parent_domain and str(parent_domain).strip() else None
                    domain_description = str(domain_description).strip() if domain_description and str(domain_description).strip() else None

                    # Create cache key: (platform, database, schema, table)
                    cache_key = (platform, database, schema, table)
                    mapping[cache_key] = domain_id

                    # Store domain info if not already stored
                    if domain_id not in domain_info_map:
                        domain_info_map[domain_id] = DomainInfo(
                            domain_id=domain_id,
                            domain_name=domain_id,  # Use domain_id as name if not provided
                            parent_domain=parent_domain,
                            domain_description=domain_description
                        )

                except Exception as e:
                    logger.error(f"Row {idx}: Error processing row: {e}")

            logger.info(f"Loaded {len(mapping)} domain mappings from table: {full_table_name}")
            logger.info(f"Found {len(domain_info_map)} unique domains")

        except Exception as e:
            logger.error(f"Error loading mapping from table {full_table_name}: {e}")
            raise

        return mapping, domain_info_map

    def _parse_dataset_urn(self, urn: str) -> Optional[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
        """
        Parse dataset URN to extract components

        Args:
            urn: Dataset URN (e.g., urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))

        Returns:
            Tuple of (platform, database, schema, table) or None if parsing fails
        """
        # Pattern: urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
        pattern = r'urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)'
        match = re.match(pattern, urn)

        if not match:
            logger.debug(f"Failed to parse dataset URN: {urn}")
            return None

        platform = match.group(1)
        name = match.group(2)

        # Parse name into database.schema.table components
        name_parts = name.split('.')

        if len(name_parts) == 1:
            # Just table name
            return (platform, None, None, name_parts[0])
        elif len(name_parts) == 2:
            # For 2 parts, treat as database.table (common for Teradata, BigQuery, etc.)
            # This matches the CSV structure where we have DataBaseName and TableName
            return (platform, name_parts[0], None, name_parts[1])
        elif len(name_parts) == 3:
            # database.schema.table
            return (platform, name_parts[0], name_parts[1], name_parts[2])
        else:
            # More than 3 parts - take last 3 as database.schema.table
            return (platform, '.'.join(name_parts[:-2]), name_parts[-2], name_parts[-1])

    def _find_matching_domain(
        self,
        platform: str,
        database: Optional[str],
        schema: Optional[str],
        table: str
    ) -> Optional[str]:
        """
        Find matching domain for dataset components

        Args:
            platform: Platform name
            database: Database name (optional)
            schema: Schema name (optional)
            table: Table name

        Returns:
            Domain ID if match found, None otherwise
        """
        # Try exact match first: (platform, database, schema, table)
        cache_key = (platform, database, schema, table)
        if cache_key in self.domain_mapping:
            return self.domain_mapping[cache_key]

        # Try without schema: (platform, database, None, table)
        if schema is not None:
            cache_key = (platform, database, None, table)
            if cache_key in self.domain_mapping:
                return self.domain_mapping[cache_key]

        # Try without database: (platform, None, schema, table)
        if database is not None:
            cache_key = (platform, None, schema, table)
            if cache_key in self.domain_mapping:
                return self.domain_mapping[cache_key]

        # Try with only platform and table: (platform, None, None, table)
        if database is not None or schema is not None:
            cache_key = (platform, None, None, table)
            if cache_key in self.domain_mapping:
                return self.domain_mapping[cache_key]

        return None

    def _domain_exists(self, domain_urn: str) -> bool:
        """
        Check if a domain exists in DataHub

        Args:
            domain_urn: Domain URN to check

        Returns:
            True if domain exists, False otherwise
        """
        try:
            if self.ctx.graph:
                # Try to get domain properties
                domain_props = self.ctx.graph.get_aspect(
                    entity_urn=domain_urn,
                    aspect_type=DomainPropertiesClass
                )
                return domain_props is not None
        except Exception as e:
            logger.debug(f"Error checking if domain exists {domain_urn}: {e}")
        
        # If we can't check, assume it doesn't exist (will try to create)
        return False

    def _create_domain_if_missing(self, domain_id: str) -> str:
        """
        Create a domain if it doesn't exist

        Args:
            domain_id: Domain ID to create

        Returns:
            Domain URN
        """
        domain_urn = make_domain_urn(domain_id)

        # Skip if already created in this run
        if domain_urn in self.created_domains:
            return domain_urn

        # Check if domain exists in DataHub
        if self._domain_exists(domain_urn):
            logger.debug(f"Domain {domain_id} already exists")
            self.created_domains.add(domain_urn)
            return domain_urn

        # Get domain info from CSV
        domain_info = self.domain_info_map.get(domain_id)
        if not domain_info:
            logger.warning(f"No domain info found for {domain_id}, using defaults")
            domain_info = DomainInfo(
                domain_id=domain_id,
                domain_name=domain_id,
                parent_domain=None,
                domain_description=None
            )

        # If parent domain specified, ensure it exists first
        parent_domain_urn = None
        if domain_info.parent_domain:
            parent_domain_urn = make_domain_urn(domain_info.parent_domain)
            # Recursively ensure parent domain exists
            self._create_domain_if_missing(domain_info.parent_domain)
            parent_domain_urn = make_domain_urn(domain_info.parent_domain)

        logger.info(f"Creating domain: {domain_id} ({domain_info.domain_name})")

        # Create domain properties
        domain_properties = DomainPropertiesClass(
            name=domain_info.domain_name or domain_id,
            description=domain_info.domain_description or f"Domain for {domain_id}",
            parentDomain=parent_domain_urn
        )

        # Emit domain properties MCP
        domain_properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=domain_properties
        )

        # Emit using emitter if available
        if self.emitter:
            try:
                self.emitter.emit(domain_properties_mcp)
                logger.info(f"Successfully created domain: {domain_urn}")
            except Exception as e:
                logger.error(f"Failed to emit domain creation MCP: {e}")
        else:
            # Try to use graph client's emitter if available
            try:
                if hasattr(self.ctx, 'graph') and self.ctx.graph:
                    # Graph client might have an emitter
                    if hasattr(self.ctx.graph, 'emit'):
                        self.ctx.graph.emit(domain_properties_mcp)
                        logger.info(f"Created domain via graph client: {domain_urn}")
                    else:
                        logger.warning(f"Cannot create domain {domain_urn} - no emitter available")
            except Exception as e:
                logger.error(f"Failed to create domain via graph client: {e}")

        self.created_domains.add(domain_urn)
        return domain_urn

    def _ensure_mapping_loaded(self):
        """Lazily load mapping from table if not already loaded"""
        if self._mapping_loaded:
            return
        
        if self.config.table_name:
            try:
                logger.info("Loading domain mapping from database table (lazy load)")
                self.domain_mapping, self.domain_info_map = self._load_mapping_from_table(
                    self.config.table_name,
                    self.config.database,
                    self.config.schema,
                    self.config.mapping,
                    self.config.static
                )
                self._mapping_loaded = True
                logger.info(f"Successfully loaded {len(self.domain_mapping)} mappings from table")
            except Exception as e:
                logger.error(f"Failed to load mapping from table: {e}")
                raise

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[DomainsClass]
    ) -> Optional[DomainsClass]:
        """
        Transform the domains aspect for a dataset
        
        Args:
            entity_urn: Entity URN
            aspect_name: Aspect name (should be "domains")
            aspect: Existing domains aspect (may be None)
            
        Returns:
            Transformed DomainsClass or None
        """
        # Ensure mapping is loaded (lazy load for table-based mapping)
        self._ensure_mapping_loaded()
        
        # Only process dataset URNs
        if not entity_urn.startswith('urn:li:dataset:'):
            return aspect

        # Parse dataset URN
        parsed = self._parse_dataset_urn(entity_urn)
        if not parsed:
            return aspect

        platform, database, schema, table = parsed

        logger.debug(f"Parsed URN {entity_urn}: platform={platform}, database={database}, schema={schema}, table={table}")

        # Find matching domain
        domain_id = self._find_matching_domain(platform, database, schema, table)

        if not domain_id:
            logger.debug(f"No CSV match found for dataset {entity_urn} (platform={platform}, database={database}, schema={schema}, table={table})")
            return aspect

        logger.info(f"Matched dataset {entity_urn} to domain {domain_id} (platform={platform}, database={database}, schema={schema}, table={table})")
        
        # Ensure domain exists (create if missing)
        domain_urn = self._create_domain_if_missing(domain_id)
        
        # Get existing domains or create new
        if aspect is None:
            domains = DomainsClass(domains=[])
        else:
            domains = aspect

        # Add domain if not already present
        if domain_urn not in domains.domains:
            if self.config.semantics == "PATCH":
                # PATCH: Add to existing domains
                domains.domains.append(domain_urn)
                logger.info(f"Adding domain {domain_urn} to dataset {entity_urn}")
            else:
                # REPLACE: Replace all domains with this one
                domains = DomainsClass(domains=[domain_urn])
                logger.info(f"Replacing domains with {domain_urn} for dataset {entity_urn}")

        return domains
