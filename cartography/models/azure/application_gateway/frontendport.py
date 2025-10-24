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
class AzureApplicationGatewayFrontendPortProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    port: PropertyRef = PropertyRef('port')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendPortToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendPortToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayFrontendPortToGatewayRelProperties = AzureAppGatewayFrontendPortToGatewayRelProperties()

@dataclass(frozen=True)
class AzureAppGatewayFrontendPortToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendPortToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayFrontendPortToSubscriptionRelProperties = AzureAppGatewayFrontendPortToSubscriptionRelProperties()

@dataclass(frozen=True)
class AzureApplicationGatewayFrontendPortSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayFrontendPort'
    properties: AzureApplicationGatewayFrontendPortProperties = AzureApplicationGatewayFrontendPortProperties()
    sub_resource_relationship: AzureAppGatewayFrontendPortToSubscriptionRel = AzureAppGatewayFrontendPortToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayFrontendPortToGatewayRel(),
        ],
    )
