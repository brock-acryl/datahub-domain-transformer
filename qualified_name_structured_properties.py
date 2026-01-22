"""
Universal transformer that adds structured properties to entities based on qualified name parsing.

Extracts components from entity URNs (database, table, column, etc.) and adds them as
structured properties. Only adds properties that are explicitly configured.
"""

from typing import List, Optional, Dict, Any, Iterable
from datahub.ingestion.transformer.base_transformer import BaseTransformer
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.configuration.common import ConfigModel
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass
)


class QualifiedNameStructuredPropertiesConfig(ConfigModel):
    """Configuration for QualifiedNameStructuredProperties transformer."""
    
    environment: str = "prod"
    platform_prefix: str = "teradata.ndw.prod"
    structured_property_urns: Optional[Dict[str, str]] = None


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
    
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "QualifiedNameStructuredProperties":
        config = QualifiedNameStructuredPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)
    
    def entity_types(self) -> List[str]:
        return ["*"]
    
    def _parse_qualified_name(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """Parse entity URN to extract qualified name components."""
        try:
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
                            parts = name_part.split('.')
                            if len(parts) == 2:
                                return {
                                    'type': 'table',
                                    'database': parts[0],
                                    'table': parts[1],
                                    'platform': platform_part,
                                    'environment': env,
                                    'qualified_name': qualified_name
                                }
                            elif len(parts) == 3:
                                return {
                                    'type': 'column',
                                    'database': parts[0],
                                    'table': parts[1],
                                    'column': parts[2],
                                    'platform': platform_part,
                                    'environment': env,
                                    'qualified_name': qualified_name
                                }
            
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
    
    def _create_structured_properties(self, parsed_info: Dict[str, Any]) -> StructuredPropertiesClass:
        """Create structured properties from parsed info. Only adds configured properties."""
        properties = []
        
        if not self.config.structured_property_urns:
            return StructuredPropertiesClass(properties=[])
        
        def add_property_if_configured(property_name: str, value: Any):
            if property_name in self.config.structured_property_urns:
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=self._get_property_urn(property_name),
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
    
    def transform(self, record_envelopes: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """Transform records by adding structured properties based on qualified name parsing."""
        for record_envelope in record_envelopes:
            try:
                record = record_envelope.record
                entity_urn = None
                snapshot = None
                
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
