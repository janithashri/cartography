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
class AzureApplicationGatewayBackendPoolProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    # backend_addresses could be complex, maybe store as string for initial step
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayBackendPoolToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayBackendPoolToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayBackendPoolToGatewayRelProperties = AzureAppGatewayBackendPoolToGatewayRelProperties()

@dataclass(frozen=True)
class AzureAppGatewayBackendPoolToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayBackendPoolToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayBackendPoolToSubscriptionRelProperties = AzureAppGatewayBackendPoolToSubscriptionRelProperties()

@dataclass(frozen=True)
class AzureApplicationGatewayBackendPoolSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayBackendPool'
    properties: AzureApplicationGatewayBackendPoolProperties = AzureApplicationGatewayBackendPoolProperties()
    sub_resource_relationship: AzureAppGatewayBackendPoolToSubscriptionRel = AzureAppGatewayBackendPoolToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayBackendPoolToGatewayRel(),
        ],
    )
