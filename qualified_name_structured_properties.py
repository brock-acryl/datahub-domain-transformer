"""
Universal transformer that adds structured properties to entities based on qualified name parsing.

Extracts components from entity URNs (database, table, column, etc.) and adds them as
structured properties. Only adds properties that are explicitly configured.

This transformer processes entities and adds structured properties based on qualified name parsing.
For columns (schemaFields), it processes the schemaMetadata aspect and creates separate MCPs
for each column's structured properties.
"""

import json
import logging
from typing import List, Literal, Optional, Dict, Any, Iterable
from datahub.ingestion.transformer.base_transformer import BaseTransformer
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    ContainerPropertiesClass,
    SchemaMetadataClass
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

logger = logging.getLogger(__name__)

try:
    from datahub.specific.dataset import DatasetPatchBuilder
    _DATASET_PATCH_AVAILABLE = True
except ImportError:
    DatasetPatchBuilder = None
    _DATASET_PATCH_AVAILABLE = False


class QualifiedNameStructuredPropertiesConfig(ConfigModel):
    """Configuration for QualifiedNameStructuredProperties transformer."""

    environment: str = "prod"
    platform_prefix: str = "teradata.ndw.prod"
    structured_property_urns: Optional[Dict[str, str]] = None
    semantics: Literal["PATCH", "REPLACE"] = "PATCH"


class QualifiedNameStructuredProperties(BaseTransformer):
    """
    Universal transformer that adds structured properties to entities based on qualified name parsing.
    
    Parses entity URNs to extract components (database, table, column, etc.) and adds them
    as structured properties. Only properties explicitly configured in structured_property_urns
    will be added.
    """
    
    def __init__(self, config: QualifiedNameStructuredPropertiesConfig, ctx: PipelineContext):
        super().__init__()
        self.config = config
        self.ctx = ctx
        self.graph = ctx.graph if hasattr(ctx, 'graph') else None
        self._exists_cache: Dict[str, bool] = {}
        self._structured_props_cache: Dict[str, Optional[StructuredPropertiesClass]] = {}
    
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "QualifiedNameStructuredProperties":
        config = QualifiedNameStructuredPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)
    
    def entity_types(self) -> List[str]:
        return ["*"]
    
    def _parse_qualified_name(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """Parse entity URN to extract qualified name components."""
        try:
            # Handle column URNs: urn:li:schemaField:(urn:li:dataset:(...),column_name)
            if 'schemafield' in entity_urn.lower():
                # Extract column name from outer parentheses (last component before closing paren)
                last_comma_idx = entity_urn.rfind(',')
                last_paren_idx = entity_urn.rfind(')')
                if last_comma_idx > 0 and last_paren_idx > last_comma_idx:
                    column_name = entity_urn[last_comma_idx + 1:last_paren_idx].strip()
                    
                    # Extract dataset URN from nested structure
                    dataset_start = entity_urn.find('urn:li:dataset:')
                    if dataset_start >= 0:
                        # Find the matching closing paren for the dataset URN
                        dataset_part = entity_urn[dataset_start:]
                        paren_count = 0
                        dataset_end = -1
                        for i, char in enumerate(dataset_part):
                            if char == '(':
                                paren_count += 1
                            elif char == ')':
                                paren_count -= 1
                                if paren_count == 0:
                                    dataset_end = i + 1
                                    break
                        
                        if dataset_end > 0:
                            dataset_urn = dataset_part[:dataset_end]
                            # Parse dataset URN directly (don't recurse to avoid issues)
                            if '(' in dataset_urn and ')' in dataset_urn:
                                parts = dataset_urn.split('(')[1].split(')')[0]
                                components = [p.strip() for p in parts.split(',')]
                                
                                if len(components) >= 2:
                                    qualified_name = components[1]
                                    env = components[2] if len(components) > 2 else self.config.environment
                                    
                                    # Check if qualified name has @ symbol
                                    if '@' in qualified_name:
                                        name_part, platform_part = qualified_name.rsplit('@', 1)
                                    else:
                                        name_part = qualified_name
                                        platform_part = self.config.platform_prefix
                                    
                                    # Parse name part to get database and table
                                    if '.' in name_part:
                                        name_parts = name_part.split('.')
                                        if len(name_parts) >= 2:
                                            database = name_parts[0]
                                            table = name_parts[1]
                                            qualified_name_full = f"{database}.{table}.{column_name}@{platform_part}"
                                            
                                            return {
                                                'type': 'column',
                                                'database': database,
                                                'table': table,
                                                'column': column_name,
                                                'platform': platform_part,
                                                'environment': env,
                                                'qualified_name': qualified_name_full
                                            }
            
            # Handle standard URNs with parentheses: urn:li:entity:(platform,qualified_name,env)
            if '(' in entity_urn and ')' in entity_urn:
                parts = entity_urn.split('(')[1].split(')')[0]
                components = [p.strip() for p in parts.split(',')]
                
                if len(components) >= 2:
                    qualified_name = components[1]
                    env = components[2] if len(components) > 2 else self.config.environment
                    
                    if '@' in qualified_name:
                        name_part, platform_part = qualified_name.rsplit('@', 1)
                        
                        if '.' not in name_part:
                            return {
                                'type': 'application' if name_part.isdigit() else 'database',
                                'name': name_part,
                                'platform': platform_part,
                                'environment': env,
                                'qualified_name': qualified_name
                            }
                        else:
                            name_parts = name_part.split('.')
                            if len(name_parts) == 2:
                                return {
                                    'type': 'table',
                                    'database': name_parts[0],
                                    'table': name_parts[1],
                                    'platform': platform_part,
                                    'environment': env,
                                    'qualified_name': qualified_name
                                }
                            elif len(name_parts) == 3:
                                return {
                                    'type': 'column',
                                    'database': name_parts[0],
                                    'table': name_parts[1],
                                    'column': name_parts[2],
                                    'platform': platform_part,
                                    'environment': env,
                                    'qualified_name': qualified_name
                                }
            
            # Fallback for dataset URNs without @ in qualified name
            if 'dataset' in entity_urn.lower():
                if ',' in entity_urn:
                    parts = entity_urn.split(',')
                    if len(parts) >= 2:
                        qualified_name = parts[1].strip().rstrip(')')
                        if '.' in qualified_name:
                            name_parts = qualified_name.split('.')
                            if len(name_parts) >= 2:
                                return {
                                    'type': 'table',
                                    'database': name_parts[0],
                                    'table': name_parts[1],
                                    'platform': self.config.platform_prefix,
                                    'environment': self.config.environment,
                                    'qualified_name': f"{qualified_name}@{self.config.platform_prefix}"
                                }
            
            return None
            
        except Exception as e:
            print(f"Error parsing qualified name from URN {entity_urn}: {e}")
            return None
    
    def _get_property_urn(self, property_name: str) -> str:
        """Get the URN for a structured property from config or use default format."""
        if self.config.structured_property_urns and property_name in self.config.structured_property_urns:
            return self.config.structured_property_urns[property_name]
        return f"urn:li:structuredProperty:{property_name}"

    def _structured_property_definition_exists(self, property_urn: str) -> bool:
        """Return True if the structured property definition exists in DataHub (or we cannot check). Cached per run."""
        if not property_urn or not property_urn.startswith("urn:li:structuredProperty:"):
            return True
        if property_urn in self._exists_cache:
            return self._exists_cache[property_urn]
        out = True
        if self.graph:
            try:
                exists = getattr(self.graph, "exists", None)
                if callable(exists):
                    out = exists(property_urn)
            except Exception:
                pass
        self._exists_cache[property_urn] = out
        return out

    def _create_structured_properties(self, parsed_info: Dict[str, Any]) -> StructuredPropertiesClass:
        """Create structured properties from parsed info. Only adds configured properties."""
        properties = []
        
        if not self.config.structured_property_urns:
            return StructuredPropertiesClass(properties=[])
        
        def add_property_if_configured(property_name: str, value: Any):
            if property_name not in self.config.structured_property_urns:
                return
            urn = self._get_property_urn(property_name)
            if not self._structured_property_definition_exists(urn):
                return
            properties.append(
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=urn,
                    values=[value] if not isinstance(value, list) else value
                )
            )
        
        if 'qualified_name' in parsed_info:
            add_property_if_configured("qualified_name", parsed_info['qualified_name'])
        
        if 'environment' in parsed_info:
            add_property_if_configured("environment", parsed_info['environment'])
        
        if 'platform' in parsed_info:
            add_property_if_configured("platform", parsed_info['platform'])
        
        entity_type = parsed_info.get('type', 'unknown')
        add_property_if_configured("entity_type", entity_type)
        
        if entity_type == 'application' and 'name' in parsed_info:
            add_property_if_configured("application_id", parsed_info['name'])
        elif entity_type == 'database' and 'name' in parsed_info:
            add_property_if_configured("database_name", parsed_info['name'])
        elif entity_type == 'table':
            if 'database' in parsed_info:
                add_property_if_configured("database_name", parsed_info['database'])
            if 'table' in parsed_info:
                add_property_if_configured("table_name", parsed_info['table'])
        elif entity_type == 'column':
            if 'database' in parsed_info:
                add_property_if_configured("database_name", parsed_info['database'])
            if 'table' in parsed_info:
                add_property_if_configured("table_name", parsed_info['table'])
            if 'column' in parsed_info:
                add_property_if_configured("column_name", parsed_info['column'])
        
        return StructuredPropertiesClass(properties=properties)

    def _get_existing_structured_properties(self, entity_urn: str, record: object) -> Optional[StructuredPropertiesClass]:
        """Get existing structuredProperties from record (e.g. MCP aspect) or graph. Graph result cached per run."""
        if isinstance(record, MetadataChangeProposalWrapper) and hasattr(record, "aspect") and isinstance(record.aspect, StructuredPropertiesClass):
            if record.aspect.properties:
                logger.debug("QualifiedNameStructuredProperties: existing from mcp for %s: %s", entity_urn, [p.propertyUrn for p in record.aspect.properties])
            return record.aspect
        if hasattr(record, "proposal") and record.proposal and hasattr(record.proposal, "aspect") and isinstance(record.proposal.aspect, StructuredPropertiesClass):
            return record.proposal.aspect
        if entity_urn in self._structured_props_cache:
            return self._structured_props_cache[entity_urn]
        asp = None
        if self.graph:
            try:
                asp = self.graph.get_aspect(entity_urn=entity_urn, aspect_type=StructuredPropertiesClass)
                if asp is not None and asp.properties:
                    logger.debug(
                        "QualifiedNameStructuredProperties: existing from graph for %s: %s",
                        entity_urn,
                        [p.propertyUrn for p in asp.properties],
                    )
            except Exception:
                pass
        self._structured_props_cache[entity_urn] = asp
        return asp

    def _assignment_value(self, assignment: StructuredPropertyValueAssignmentClass) -> Any:
        """Extract a single value for patch (string or number)."""
        if not assignment.values:
            return ""
        v = assignment.values[0]
        if isinstance(v, dict):
            if "string" in v and v["string"] is not None:
                return v["string"]
            if "double" in v and v["double"] is not None:
                return v["double"]
        if hasattr(v, "string") and getattr(v, "string", None) is not None:
            return v.string
        if hasattr(v, "double") and getattr(v, "double", None) is not None:
            return v.double
        return str(v) if v else ""

    def _emit_dataset_structured_props(self, entity_urn: str, new_props: StructuredPropertiesClass, record_envelope: RecordEnvelope):
        """Emit structured properties for a dataset. When PATCH: use patch builder if available (no lookup), else emit only new_props. When not PATCH: lookup and merge."""
        if self.config.semantics == "PATCH":
            if _DATASET_PATCH_AVAILABLE and new_props.properties:
                patch_builder = DatasetPatchBuilder(entity_urn)
                for p in new_props.properties:
                    patch_builder.add_structured_property(p.propertyUrn, self._assignment_value(p))
                for patch_mcp in patch_builder.build():
                    yield RecordEnvelope(record=patch_mcp, metadata=record_envelope.metadata)
            else:
                yield RecordEnvelope(
                    record=MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=new_props),
                    metadata=record_envelope.metadata,
                )
            return
        existing = self._get_existing_structured_properties(entity_urn, record_envelope.record)
        aspect_to_send = self._merge_structured_props(new_props, existing)
        yield RecordEnvelope(
            record=MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect_to_send),
            metadata=record_envelope.metadata,
        )

    def _merge_structured_props(self, new_props: StructuredPropertiesClass, existing: Optional[StructuredPropertiesClass]) -> StructuredPropertiesClass:
        """Merge new_props into existing when semantics is PATCH; otherwise return new_props."""
        if self.config.semantics != "PATCH" or not existing or not existing.properties:
            return new_props
        existing_filtered = [
            p for p in existing.properties
            if self._structured_property_definition_exists(p.propertyUrn)
        ]
        merged = list(existing_filtered)
        existing_urns = {p.propertyUrn for p in merged}
        for p in new_props.properties:
            if p.propertyUrn in existing_urns:
                merged = [p if x.propertyUrn == p.propertyUrn else x for x in merged]
            else:
                merged.append(p)
                existing_urns.add(p.propertyUrn)
        return StructuredPropertiesClass(properties=merged)

    def _log_incoming_structured_properties(self, record: object, entity_urn: Optional[str]) -> None:
        """Log if the incoming record already has structured properties (from source or earlier in pipeline)."""
        urns = None
        source = None
        if isinstance(record, MetadataChangeProposalWrapper) and hasattr(record, "aspect") and isinstance(record.aspect, StructuredPropertiesClass) and record.aspect.properties:
            urns = [p.propertyUrn for p in record.aspect.properties]
            source = "incoming_mcp"
        elif hasattr(record, "proposedSnapshot") and record.proposedSnapshot and hasattr(record.proposedSnapshot, "aspects"):
            for asp in record.proposedSnapshot.aspects:
                if isinstance(asp, StructuredPropertiesClass) and asp.properties:
                    urns = [p.propertyUrn for p in asp.properties]
                    source = "incoming_proposedSnapshot"
                    break
        elif hasattr(record, "snapshot") and record.snapshot and hasattr(record.snapshot, "aspects"):
            for asp in record.snapshot.aspects:
                if isinstance(asp, StructuredPropertiesClass) and asp.properties:
                    urns = [p.propertyUrn for p in asp.properties]
                    source = "incoming_snapshot"
                    break
        if urns is not None and entity_urn and "dataset" in (entity_urn or "").lower():
            logger.info(
                "QualifiedNameStructuredProperties: record already has structured properties from %s for %s: %s",
                source,
                entity_urn,
                urns,
            )

    def transform(self, record_envelopes: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """Transform records by adding structured properties based on qualified name parsing."""
        for record_envelope in record_envelopes:
            try:
                record = record_envelope.record
                entity_urn = None
                snapshot = None
                if hasattr(record, "entityUrn"):
                    entity_urn = record.entityUrn
                elif hasattr(record, "proposal") and record.proposal and hasattr(record.proposal, "entityUrn"):
                    entity_urn = record.proposal.entityUrn
                elif hasattr(record, "urn"):
                    entity_urn = record.urn
                elif hasattr(record, "proposedSnapshot") and record.proposedSnapshot and hasattr(record.proposedSnapshot, "urn"):
                    entity_urn = record.proposedSnapshot.urn
                self._log_incoming_structured_properties(record, entity_urn)

                # Handle MCPs (MetadataChangeProposalWrapper) - containers often come as MCPs
                if isinstance(record, MetadataChangeProposalWrapper):
                    # MCPs can have entityUrn and aspect directly, or in a proposal attribute
                    entity_urn = None
                    aspect = None
                    aspect_name = None
                    
                    # Check direct attributes first
                    if hasattr(record, 'entityUrn'):
                        entity_urn = record.entityUrn
                    if hasattr(record, 'aspect'):
                        aspect = record.aspect
                    if hasattr(record, 'aspectName'):
                        aspect_name = record.aspectName
                    
                    # Check proposal attribute
                    if not entity_urn and hasattr(record, 'proposal') and record.proposal:
                        entity_urn = record.proposal.entityUrn if hasattr(record.proposal, 'entityUrn') else None
                        aspect = record.proposal.aspect if hasattr(record.proposal, 'aspect') else None
                        aspect_name = record.proposal.aspectName if hasattr(record.proposal, 'aspectName') else None
                    
                    # For MCPs, the aspect is in proposal.aspect
                    if 'container' in (entity_urn or '').lower():
                        database_name = None
                        env = self.config.environment
                        if aspect and isinstance(aspect, ContainerPropertiesClass):
                            database_name = aspect.name
                            if aspect.env:
                                env = aspect.env
                        elif aspect and getattr(aspect, 'value', None):
                            try:
                                raw = getattr(aspect, 'value')
                                data = json.loads(raw) if isinstance(raw, str) else raw
                                database_name = data.get('name') if isinstance(data, dict) else None
                                if isinstance(data, dict) and data.get('env'):
                                    env = data['env']
                            except (json.JSONDecodeError, TypeError):
                                pass
                        if database_name:
                            qualified_name = f"{database_name}@{self.config.platform_prefix}"
                            parsed_info = {
                                'type': 'database',
                                'name': database_name,
                                'platform': self.config.platform_prefix,
                                'environment': env,
                                'qualified_name': qualified_name
                            }
                            new_props = self._create_structured_properties(parsed_info)
                            if new_props.properties:
                                if self.config.semantics == "PATCH":
                                    aspect_to_send = new_props
                                else:
                                    existing = self._get_existing_structured_properties(entity_urn, record)
                                    aspect_to_send = self._merge_structured_props(new_props, existing)
                                structured_props_mcp = MetadataChangeProposalWrapper(
                                    entityUrn=entity_urn,
                                    aspect=aspect_to_send
                                )
                                yield record_envelope
                                yield RecordEnvelope(record=structured_props_mcp, metadata=record_envelope.metadata)
                                continue
                        yield record_envelope
                        continue
                    if 'dataset' in (entity_urn or '').lower():
                        parsed_info = self._parse_qualified_name(entity_urn)
                        if parsed_info and parsed_info.get('type') == 'table':
                            new_props = self._create_structured_properties(parsed_info)
                            if new_props.properties:
                                yield record_envelope
                                yield from self._emit_dataset_structured_props(entity_urn, new_props, record_envelope)
                                continue
                
                # Quick check: try to get entity_urn early for debugging
                if hasattr(record, 'urn') and getattr(record, 'urn', None) is not None:
                    entity_urn = record.urn
                elif hasattr(record, 'proposedSnapshot') and record.proposedSnapshot and hasattr(record.proposedSnapshot, 'urn'):
                    entity_urn = record.proposedSnapshot.urn
                elif hasattr(record, 'entityUrn'):
                    entity_urn = record.entityUrn
                
                if hasattr(record, 'urn') and getattr(record, 'urn', None) is not None:
                    snapshot = record
                    entity_urn = record.urn
                elif not hasattr(record, 'urn') and hasattr(record, 'proposedSnapshot'):
                    try:
                        if record.proposedSnapshot:
                            snapshot = record.proposedSnapshot
                            entity_urn = snapshot.urn if hasattr(snapshot, 'urn') else None
                    except AttributeError:
                        pass
                
                if not entity_urn and hasattr(record, 'entityUrn'):
                    entity_urn = record.entityUrn
                    if hasattr(record, 'snapshot'):
                        snapshot = record.snapshot
                
                if not entity_urn or not snapshot:
                    yield record_envelope
                    continue
                
                # Process schemaMetadata to add structured properties to columns
                schema_metadata = None
                if hasattr(record, 'proposedSnapshot') and record.proposedSnapshot and hasattr(record.proposedSnapshot, 'aspects'):
                    for aspect in record.proposedSnapshot.aspects:
                        if isinstance(aspect, SchemaMetadataClass):
                            schema_metadata = aspect
                            break
                elif hasattr(snapshot, 'aspects'):
                    for aspect in snapshot.aspects:
                        if isinstance(aspect, SchemaMetadataClass):
                            schema_metadata = aspect
                            break
                
                # If we have schemaMetadata, process columns
                if schema_metadata and schema_metadata.fields:
                    # Parse dataset URN to get database and table info
                    dataset_parsed = self._parse_qualified_name(entity_urn)
                    if dataset_parsed and dataset_parsed.get('type') == 'table':
                        database = dataset_parsed.get('database')
                        table = dataset_parsed.get('table')
                        platform = dataset_parsed.get('platform', self.config.platform_prefix)
                        env = dataset_parsed.get('environment', self.config.environment)
                        
                        # Process each field/column
                        for field in schema_metadata.fields:
                            if field.fieldPath:
                                column_name = field.fieldPath
                                # Build qualified name for column
                                qualified_name = f"{database}.{table}.{column_name}@{platform}"
                                column_parsed_info = {
                                    'type': 'column',
                                    'database': database,
                                    'table': table,
                                    'column': column_name,
                                    'platform': platform,
                                    'environment': env,
                                    'qualified_name': qualified_name
                                }
                                
                                # Create structured properties for this column
                                column_props = self._create_structured_properties(column_parsed_info)
                                if column_props.properties:
                                    # Create schemaField URN: urn:li:schemaField:(dataset_urn,fieldPath)
                                    from datahub.emitter.mce_builder import make_schema_field_urn
                                    schema_field_urn = make_schema_field_urn(entity_urn, field.fieldPath)
                                    
                                    # Create MCP for column structured properties
                                    column_mcp = MetadataChangeProposalWrapper(
                                        entityUrn=schema_field_urn,
                                        aspect=column_props
                                    )
                                    yield RecordEnvelope(record=column_mcp, metadata=record_envelope.metadata)
                
                # Parse URN for entity-level structured properties
                parsed_info = self._parse_qualified_name(entity_urn)
                
                if not parsed_info:
                    yield record_envelope
                    continue
                
                structured_props = None
                
                if hasattr(record, 'proposedSnapshot') and record.proposedSnapshot is not None:
                    if hasattr(record.proposedSnapshot, 'aspects'):
                        existing_props = None
                        for aspect in record.proposedSnapshot.aspects:
                            if isinstance(aspect, StructuredPropertiesClass):
                                existing_props = aspect
                                break
                        
                        if existing_props:
                            structured_props = existing_props
                        else:
                            structured_props = StructuredPropertiesClass(properties=[])
                            record.proposedSnapshot.aspects.append(structured_props)
                    else:
                        yield record_envelope
                        continue
                elif hasattr(snapshot, 'aspects'):
                    existing_props = None
                    for aspect in snapshot.aspects:
                        if isinstance(aspect, StructuredPropertiesClass):
                            existing_props = aspect
                            break
                    
                    if existing_props:
                        structured_props = existing_props
                    else:
                        structured_props = StructuredPropertiesClass(properties=[])
                        snapshot.aspects.append(structured_props)
                else:
                    yield record_envelope
                    continue
                
                new_props = self._create_structured_properties(parsed_info)
                existing_urns = {prop.propertyUrn for prop in structured_props.properties}
                for prop in new_props.properties:
                    if prop.propertyUrn not in existing_urns:
                        structured_props.properties.append(prop)
                
                yield record_envelope
                
            except Exception as e:
                print(f"ERROR in transform(): {e}")
                import traceback
                traceback.print_exc()
                yield record_envelope
