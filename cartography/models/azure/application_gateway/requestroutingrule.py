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

# --- (Your existing Properties and Rel classes are correct) ---

@dataclass(frozen=True)
class AzureApplicationGatewayRequestRoutingRuleProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    rule_type: PropertyRef = PropertyRef('rule_type')
    priority: PropertyRef = PropertyRef('priority')
    listener_id: PropertyRef = PropertyRef('listener_id')
    backend_pool_id: PropertyRef = PropertyRef('backend_pool_id')
    backend_setting_id: PropertyRef = PropertyRef('backend_setting_id')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayRuleToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayRuleToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayRuleToGatewayRelProperties = AzureAppGatewayRuleToGatewayRelProperties()


@dataclass(frozen=True)
class AzureAppGatewayRuleToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayRuleToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayRuleToSubscriptionRelProperties = AzureAppGatewayRuleToSubscriptionRelProperties()

# --- START OF FIX ---

# Create specific property classes for each relationship
@dataclass(frozen=True)
class RuleToListenerRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class RuleToBackendPoolRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class RuleToBackendSettingRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

# Relationship: Rule -> Listener
@dataclass(frozen=True)
class RuleToListenerRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayListener'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('listener_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'ASSOCIATED_LISTENER'
    # Use the new specific property class
    properties: RuleToListenerRelProperties = RuleToListenerRelProperties()

# Relationship: Rule -> Backend Pool
@dataclass(frozen=True)
class RuleToBackendPoolRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayBackendPool'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('backend_pool_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'ROUTES_TO_BACKEND_POOL'
    # Use the new specific property class
    properties: RuleToBackendPoolRelProperties = RuleToBackendPoolRelProperties()

# Relationship: Rule -> Backend Setting
@dataclass(frozen=True)
class RuleToBackendSettingRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayBackendSetting'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('backend_setting_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'ROUTES_TO_BACKEND_SETTING'
    # Use the new specific property class
    properties: RuleToBackendSettingRelProperties = RuleToBackendSettingRelProperties()

# --- END OF FIX ---

@dataclass(frozen=True)
class AzureApplicationGatewayRequestRoutingRuleSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayRequestRoutingRule'
    properties: AzureApplicationGatewayRequestRoutingRuleProperties = AzureApplicationGatewayRequestRoutingRuleProperties()
    sub_resource_relationship: AzureAppGatewayRuleToSubscriptionRel = AzureAppGatewayRuleToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayRuleToGatewayRel(),
            RuleToListenerRel(),
            RuleToBackendPoolRel(),
            RuleToBackendSettingRel(),
        ],
    )