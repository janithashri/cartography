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

# --- (Your existing Properties, ToGatewayRel, and ToSubscriptionRel classes are all correct) ---

@dataclass(frozen=True)
class AzureApplicationGatewayBackendSettingProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    port: PropertyRef = PropertyRef('port')
    protocol: PropertyRef = PropertyRef('protocol')
    cookie_based_affinity: PropertyRef = PropertyRef('cookie_based_affinity')
    probe_id: PropertyRef = PropertyRef('probe_id') # ID of the health probe
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayBackendSettingToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayBackendSettingToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayBackendSettingToGatewayRelProperties = AzureAppGatewayBackendSettingToGatewayRelProperties()


@dataclass(frozen=True)
class AzureAppGatewayBackendSettingToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayBackendSettingToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayBackendSettingToSubscriptionRelProperties = AzureAppGatewayBackendSettingToSubscriptionRelProperties()


# --- START OF FIX ---

# 1. Create a new, specific properties class for the relationship
@dataclass(frozen=True)
class BackendSettingToHealthProbeRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


# 2. Update the relationship schema to use the new class
@dataclass(frozen=True)
class BackendSettingToHealthProbeRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayHealthProbe'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('probe_id')}, # Use the probe_id property from the node
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'USES_HEALTH_PROBE'
    # Use the new specific class here
    properties: BackendSettingToHealthProbeRelProperties = BackendSettingToHealthProbeRelProperties()

# --- END OF FIX ---


@dataclass(frozen=True)
class AzureApplicationGatewayBackendSettingSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayBackendSetting'
    properties: AzureApplicationGatewayBackendSettingProperties = AzureApplicationGatewayBackendSettingProperties()
    sub_resource_relationship: AzureAppGatewayBackendSettingToSubscriptionRel = AzureAppGatewayBackendSettingToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayBackendSettingToGatewayRel(),
            BackendSettingToHealthProbeRel(),
        ],
    )