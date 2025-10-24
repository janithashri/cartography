from dataclasses import dataclass
from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties
from cartography.models.core.nodes import CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties
from cartography.models.core.relationships import CartographyRelSchema
from cartography.models.core.relationships import LinkDirection
from cartography.models.core.relationships import make_target_node_matcher
from cartography.models.core.relationships import TargetNodeMatcher

@dataclass(frozen=True)
class AzureApplicationGatewayProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    location: PropertyRef = PropertyRef('location')
    sku_name: PropertyRef = PropertyRef('sku_name')
    provisioning_state: PropertyRef = PropertyRef('provisioning_state')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureApplicationGatewayToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class AzureApplicationGatewayToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureApplicationGatewayToSubscriptionRelProperties = AzureApplicationGatewayToSubscriptionRelProperties()

@dataclass(frozen=True)
class AzureApplicationGatewaySchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGateway'
    properties: AzureApplicationGatewayProperties = AzureApplicationGatewayProperties()
    sub_resource_relationship: AzureApplicationGatewayToSubscriptionRel = AzureApplicationGatewayToSubscriptionRel()
