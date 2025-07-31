import logging
from dataclasses import dataclass
from typing import Optional
from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties, CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties, CartographyRelSchema, LinkDirection, make_target_node_matcher, TargetNodeMatcher, OtherRelationships

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class GCPCloudFunctionProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('name', extra_index=True)
    name: PropertyRef = PropertyRef('name')
    project_id: PropertyRef = PropertyRef('PROJECT_ID', set_in_kwargs=True)
    region: PropertyRef = PropertyRef('region', set_in_kwargs=True)
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class GCPCloudFunctionToGCPProjectRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class GCPCloudFunctionToGCPProjectRel(CartographyRelSchema):
    target_node_label: str = "GCPProject"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"id": PropertyRef("PROJECT_ID", set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = "RESOURCE"
    properties: GCPCloudFunctionToGCPProjectRelProperties = GCPCloudFunctionToGCPProjectRelProperties()

@dataclass(frozen=True)
class GCPCloudFunctionNode(CartographyNodeSchema):
    label: str = "GCPCloudFunction"
    properties: GCPCloudFunctionProperties = GCPCloudFunctionProperties()
    sub_resource_relationship: Optional[GCPCloudFunctionToGCPProjectRel] = GCPCloudFunctionToGCPProjectRel()
    other_relationships: Optional[OtherRelationships] = None