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
class AzureApplicationGatewayFrontendIPConfigurationProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    private_ip_address: PropertyRef = PropertyRef('private_ip_address')
    public_ip_address_id: PropertyRef = PropertyRef('public_ip_address_id')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendIPToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendIPToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayFrontendIPToGatewayRelProperties = AzureAppGatewayFrontendIPToGatewayRelProperties()

@dataclass(frozen=True)
class AzureAppGatewayFrontendIPToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureAppGatewayFrontendIPToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayFrontendIPToSubscriptionRelProperties = AzureAppGatewayFrontendIPToSubscriptionRelProperties()

@dataclass(frozen=True)
class AzureApplicationGatewayFrontendIPConfigurationSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayFrontendIPConfiguration'
    properties: AzureApplicationGatewayFrontendIPConfigurationProperties = AzureApplicationGatewayFrontendIPConfigurationProperties()
    sub_resource_relationship: AzureAppGatewayFrontendIPToSubscriptionRel = AzureAppGatewayFrontendIPToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayFrontendIPToGatewayRel(),
        ],
    )
