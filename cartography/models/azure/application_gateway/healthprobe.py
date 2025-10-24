from dataclasses import dataclass
from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties
from cartography.models.core.nodes import CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties
from cartography.models.core.relationships import CartographyRelSchema
from cartography.models.core.relationships import LinkDirection
from cartography.models.core.relationships import make_target_node_matcher
from cartography.models.core.relationships import OtherRelationships
from cartography.models.core.relationships import TargetNodeMatcher

@dataclass(frozen=True)
class AzureApplicationGatewayHealthProbeProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    protocol: PropertyRef = PropertyRef('protocol')
    path: PropertyRef = PropertyRef('path')
    interval: PropertyRef = PropertyRef('interval')
    timeout: PropertyRef = PropertyRef('timeout')
    unhealthy_threshold: PropertyRef = PropertyRef('unhealthy_threshold')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayHealthProbeToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayHealthProbeToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayHealthProbeToGatewayRelProperties = AzureAppGatewayHealthProbeToGatewayRelProperties()

@dataclass(frozen=True)
class AzureAppGatewayHealthProbeToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayHealthProbeToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayHealthProbeToSubscriptionRelProperties = AzureAppGatewayHealthProbeToSubscriptionRelProperties()

@dataclass(frozen=True)
class AzureApplicationGatewayHealthProbeSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayHealthProbe'
    properties: AzureApplicationGatewayHealthProbeProperties = AzureApplicationGatewayHealthProbeProperties()
    sub_resource_relationship: AzureAppGatewayHealthProbeToSubscriptionRel = AzureAppGatewayHealthProbeToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayHealthProbeToGatewayRel(),
        ],
    )
