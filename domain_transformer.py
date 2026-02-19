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
from typing import Dict, Iterable, List, Literal, Optional, Tuple
from dataclasses import dataclass

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.base_transformer import BaseTransformer
from datahub.metadata.schema_classes import (
    DomainsClass,
    DomainPropertiesClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

logger = logging.getLogger(__name__)

try:
    from datahub.specific.dataset import DatasetPatchBuilder
    _PATCH_BUILDER_AVAILABLE = True
except ImportError:
    DatasetPatchBuilder = None
    _PATCH_BUILDER_AVAILABLE = False


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
    # Note: datahub_url and datahub_token are deprecated - use self.ctx.graph instead
    # Kept for backward compatibility but not used
    datahub_url: Optional[str] = None
    datahub_token: Optional[str] = None
    mapping: Dict[str, Optional[str]] = {}
    static: Dict[str, Optional[str]] = {}
    semantics: str = "PATCH"  # PATCH = add to existing, REPLACE = replace existing
    write_to_domain: bool = True
    structured_property_urn: Optional[str] = None
    structured_property_value: Literal["id", "urn"] = "id"


class DomainTransformer(BaseTransformer):
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
        self._exists_cache: Dict[str, bool] = {}
        self._structured_props_cache: Dict[str, Optional[StructuredPropertiesClass]] = {}
        
        # Use the graph client from PipelineContext - it's already configured with
        # the sink's server URL and authentication token, so no need to configure separately
        if not self.ctx.graph:
            logger.warning("PipelineContext.graph is not available - domain creation will not work")
        else:
            logger.info("Using PipelineContext.graph for domain creation (already configured with sink connection)")
        
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

    def _get_entity_urn_from_record(self, record: object) -> Optional[str]:
        """Extract entity URN from an MCE or MCP record."""
        if hasattr(record, "entityUrn") and getattr(record, "entityUrn", None):
            return record.entityUrn
        if hasattr(record, "proposal") and getattr(record, "proposal", None):
            p = record.proposal
            if hasattr(p, "entityUrn") and getattr(p, "entityUrn", None):
                return p.entityUrn
        if hasattr(record, "urn") and getattr(record, "urn", None):
            return record.urn
        if hasattr(record, "proposedSnapshot") and getattr(record, "proposedSnapshot", None):
            snap = record.proposedSnapshot
            if hasattr(snap, "urn") and getattr(snap, "urn", None):
                return snap.urn
        return None

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
            logger.debug(f"Ensuring parent domain exists: {domain_info.parent_domain}")
            self._create_domain_if_missing(domain_info.parent_domain)
            # Verify parent domain was created/exists
            if not self._domain_exists(parent_domain_urn):
                logger.warning(f"Parent domain {domain_info.parent_domain} may not exist. Child domain creation may fail.")
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

        # Emit using graph client from PipelineContext (already configured with sink connection)
        if not self.ctx.graph:
            logger.error(f"Cannot create domain {domain_urn} - PipelineContext.graph is not available")
            raise RuntimeError("PipelineContext.graph is not available - cannot create domains")
        
        try:
            # DataHubGraph has an emit_mcp method for emitting MetadataChangeProposalWrapper
            # This uses the connection already configured from the sink
            self.ctx.graph.emit_mcp(domain_properties_mcp)
            logger.info(f"Successfully created domain: {domain_urn}")
        except AttributeError:
            # Fallback: try alternative method names if emit_mcp doesn't exist
            try:
                # Some versions might use emit or ingest_mcp
                if hasattr(self.ctx.graph, 'emit'):
                    self.ctx.graph.emit(domain_properties_mcp)
                    logger.info(f"Successfully created domain via emit: {domain_urn}")
                elif hasattr(self.ctx.graph, 'ingest_mcp'):
                    self.ctx.graph.ingest_mcp(domain_properties_mcp)
                    logger.info(f"Successfully created domain via ingest_mcp: {domain_urn}")
                else:
                    raise AttributeError("DataHubGraph does not have emit_mcp, emit, or ingest_mcp methods")
            except Exception as e:
                logger.error(f"Failed to create domain {domain_urn}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to create domain {domain_urn}: {e}")
            raise

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

    def _get_existing_domains(self, entity_urn: str, record: object) -> Optional[DomainsClass]:
        """Get existing domains aspect from graph or from record snapshot."""
        if hasattr(record, "proposedSnapshot") and record.proposedSnapshot and hasattr(record.proposedSnapshot, "aspects"):
            for asp in record.proposedSnapshot.aspects:
                if isinstance(asp, DomainsClass):
                    return asp
        if hasattr(record, "snapshot") and record.snapshot and hasattr(record.snapshot, "aspects"):
            for asp in record.snapshot.aspects:
                if isinstance(asp, DomainsClass):
                    return asp
        if isinstance(record, MetadataChangeProposalWrapper) and hasattr(record, "aspect") and isinstance(record.aspect, DomainsClass):
            return record.aspect
        if self.ctx.graph:
            try:
                return self.ctx.graph.get_aspect(entity_urn=entity_urn, aspect_type=DomainsClass)
            except Exception:
                pass
        return None

    def _get_existing_structured_properties(self, entity_urn: str, record: object) -> Optional[StructuredPropertiesClass]:
        """Get existing structuredProperties aspect from graph or from record snapshot."""
        if hasattr(record, "proposedSnapshot") and record.proposedSnapshot and hasattr(record.proposedSnapshot, "aspects"):
            for asp in record.proposedSnapshot.aspects:
                if isinstance(asp, StructuredPropertiesClass):
                    if asp.properties:
                        logger.debug(
                            "Structured properties for %s from proposedSnapshot: %s",
                            entity_urn,
                            [p.propertyUrn for p in asp.properties],
                        )
                    return asp
        if hasattr(record, "snapshot") and record.snapshot and hasattr(record.snapshot, "aspects"):
            for asp in record.snapshot.aspects:
                if isinstance(asp, StructuredPropertiesClass):
                    if asp.properties:
                        logger.debug(
                            "Structured properties for %s from snapshot: %s",
                            entity_urn,
                            [p.propertyUrn for p in asp.properties],
                        )
                    return asp
        if isinstance(record, MetadataChangeProposalWrapper) and hasattr(record, "aspect") and isinstance(record.aspect, StructuredPropertiesClass):
            if record.aspect.properties:
                logger.debug(
                    "Structured properties for %s from mcp: %s",
                    entity_urn,
                    [p.propertyUrn for p in record.aspect.properties],
                )
            return record.aspect
        if entity_urn in self._structured_props_cache:
            return self._structured_props_cache[entity_urn]
        asp = None
        if self.ctx.graph:
            try:
                asp = self.ctx.graph.get_aspect(entity_urn=entity_urn, aspect_type=StructuredPropertiesClass)
                if asp is not None and asp.properties:
                    logger.debug(
                        "Structured properties for %s from graph: %s",
                        entity_urn,
                        [p.propertyUrn for p in asp.properties],
                    )
            except Exception:
                pass
        self._structured_props_cache[entity_urn] = asp
        return asp

    def _structured_property_definition_exists(self, property_urn: str) -> bool:
        """Return True if the structured property definition exists in DataHub (or we cannot check). Cached per run."""
        if not property_urn or not property_urn.startswith("urn:li:structuredProperty:"):
            return True
        if property_urn in self._exists_cache:
            return self._exists_cache[property_urn]
        out = True
        if self.ctx.graph:
            try:
                exists = getattr(self.ctx.graph, "exists", None)
                if callable(exists):
                    out = exists(property_urn)
            except Exception as e:
                logger.debug("Could not check structured property definition %s: %s", property_urn, e)
        self._exists_cache[property_urn] = out
        return out

    def _set_structured_properties_on_record(self, record: object, structured_props: StructuredPropertiesClass) -> bool:
        """Merge structured_props into the record's snapshot so downstream transformers and sink see the full set."""
        snapshot = None
        if hasattr(record, "proposedSnapshot") and record.proposedSnapshot and hasattr(record.proposedSnapshot, "aspects"):
            snapshot = record.proposedSnapshot
        elif hasattr(record, "snapshot") and record.snapshot and hasattr(record.snapshot, "aspects"):
            snapshot = record.snapshot
        if not snapshot:
            return False
        existing_props = None
        for asp in snapshot.aspects:
            if isinstance(asp, StructuredPropertiesClass):
                existing_props = asp
                break
        if existing_props:
            existing_urns = {p.propertyUrn for p in existing_props.properties}
            for p in structured_props.properties:
                if p.propertyUrn not in existing_urns:
                    existing_props.properties.append(p)
        else:
            snapshot.aspects.append(structured_props)
        return True

    def transform(self, record_envelopes: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """Transform records: optionally add domain association and/or structured property from mapping."""
        for record_envelope in record_envelopes:
            record = record_envelope.record
            entity_urn = self._get_entity_urn_from_record(record)

            if not entity_urn or not entity_urn.startswith("urn:li:dataset:"):
                yield record_envelope
                continue

            self._ensure_mapping_loaded()
            parsed = self._parse_dataset_urn(entity_urn)
            if not parsed:
                yield record_envelope
                continue

            platform, database, schema, table = parsed
            logger.debug(f"Parsed URN {entity_urn}: platform={platform}, database={database}, schema={schema}, table={table}")

            domain_id = self._find_matching_domain(platform, database, schema, table)
            if not domain_id:
                logger.debug(f"No match for dataset {entity_urn} (platform={platform}, database={database}, schema={schema}, table={table})")
                yield record_envelope
                continue

            logger.info(f"Matched dataset {entity_urn} to domain {domain_id} (platform={platform}, database={database}, schema={schema}, table={table})")
            domain_urn = self._create_domain_if_missing(domain_id)

            if self.config.write_to_domain:
                if self.config.semantics == "PATCH":
                    domains_list: List[str] = [domain_urn]
                    logger.info(f"PATCH: adding domain {domain_urn} to dataset {entity_urn} (no lookup)")
                else:
                    existing = self._get_existing_domains(entity_urn, record)
                    if existing is None:
                        domains_list = []
                    else:
                        domains_list = list(existing.domains) if existing.domains else []
                    if domain_urn not in domains_list:
                        domains_list = [domain_urn]
                        logger.info(f"Replacing domains with {domain_urn} for dataset {entity_urn}")
                domains_mcp = MetadataChangeProposalWrapper(
                    entityUrn=entity_urn, aspect=DomainsClass(domains=domains_list)
                )
                yield RecordEnvelope(record=domains_mcp, metadata=record_envelope.metadata)

            if self.config.structured_property_urn:
                value = domain_id if self.config.structured_property_value == "id" else domain_urn
                if not value or (isinstance(value, str) and not value.strip()):
                    logger.warning(
                        "Structured property value is empty for %s (domain_id=%s); skipping structured property write",
                        entity_urn,
                        domain_id,
                    )
                else:
                    if self.config.semantics == "PATCH" and _PATCH_BUILDER_AVAILABLE:
                        patch_builder = DatasetPatchBuilder(entity_urn)
                        patch_builder.add_structured_property(self.config.structured_property_urn, value)
                        for patch_mcp in patch_builder.build():
                            yield RecordEnvelope(record=patch_mcp, metadata=record_envelope.metadata)
                    else:
                        if not self._structured_property_definition_exists(self.config.structured_property_urn):
                            logger.warning(
                                "Structured property definition does not exist for %s; skipping for %s",
                                self.config.structured_property_urn,
                                entity_urn,
                            )
                        else:
                            assignment = StructuredPropertyValueAssignmentClass(
                                propertyUrn=self.config.structured_property_urn,
                                values=[value],
                            )
                            structured_props = StructuredPropertiesClass(properties=[assignment])
                            self._set_structured_properties_on_record(record, structured_props)
                            sp_mcp = MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=structured_props)
                            yield RecordEnvelope(record=sp_mcp, metadata=record_envelope.metadata)

            yield record_envelope
