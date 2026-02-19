"""
Transformer that adds one or more structured properties with a fixed value to entities.

Config specifies structured property URN(s) and a single value; supports PATCH (add to existing)
or REPLACE (replace entire structured properties) semantics.
"""

import logging
from typing import Dict, List, Optional, Union

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)

logger = logging.getLogger(__name__)


class SimpleAddStructuredPropertiesConfig(ConfigModel):
    structured_properties_urns: Union[str, List[str]]
    value: str
    semantics: str = "PATCH"
    entity_types: Optional[List[str]] = None  # default ["dataset"] to avoid writing to tags etc.


class SimpleAddStructuredProperties(BaseTransformer, SingleAspectTransformer):
    ctx: PipelineContext
    config: SimpleAddStructuredPropertiesConfig

    def __init__(self, config: SimpleAddStructuredPropertiesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        urns = config.structured_properties_urns
        self._property_urns: List[str] = [urns] if isinstance(urns, str) else list(urns)
        self._entity_types: List[str] = config.entity_types if config.entity_types is not None else ["dataset"]
        self._exists_cache: Dict[str, bool] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddStructuredProperties":
        config = SimpleAddStructuredPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return self._entity_types

    def aspect_name(self) -> str:
        return "structuredProperties"

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
            except Exception:
                pass
        self._exists_cache[property_urn] = out
        return out

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[StructuredPropertiesClass]
    ) -> Optional[StructuredPropertiesClass]:
        assignments = [
            StructuredPropertyValueAssignmentClass(
                propertyUrn=urn,
                values=[self.config.value],
            )
            for urn in self._property_urns
        ]
        if self.config.semantics == "REPLACE":
            return StructuredPropertiesClass(properties=assignments)
        if self.config.semantics == "PATCH":
            return StructuredPropertiesClass(properties=assignments)
        existing_list = list(aspect.properties) if aspect and aspect.properties else []
        existing_list = [
            p for p in existing_list
            if self._structured_property_definition_exists(p.propertyUrn)
        ]
        for a in assignments:
            existing_list = [p for p in existing_list if p.propertyUrn != a.propertyUrn]
            existing_list.append(a)
        return StructuredPropertiesClass(properties=existing_list)
