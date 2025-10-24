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
class AzureApplicationGatewayListenerProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('id')
    name: PropertyRef = PropertyRef('name')
    protocol: PropertyRef = PropertyRef('protocol')
    frontend_ip_configuration_id: PropertyRef = PropertyRef('frontend_ip_configuration_id')
    frontend_port_id: PropertyRef = PropertyRef('frontend_port_id')
    ssl_certificate_id: PropertyRef = PropertyRef('ssl_certificate_id')
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayListenerToGatewayRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayListenerToGatewayRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGateway'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('GATEWAY_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'CONTAINS'
    properties: AzureAppGatewayListenerToGatewayRelProperties = AzureAppGatewayListenerToGatewayRelProperties()


@dataclass(frozen=True)
class AzureAppGatewayListenerToSubscriptionRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AzureAppGatewayListenerToSubscriptionRel(CartographyRelSchema):
    target_node_label: str = 'AzureSubscription'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AZURE_SUBSCRIPTION_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = 'RESOURCE'
    properties: AzureAppGatewayListenerToSubscriptionRelProperties = AzureAppGatewayListenerToSubscriptionRelProperties()


# --- START OF FIX ---

# Create specific property classes for each relationship
@dataclass(frozen=True)
class ListenerToFrontendIPRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class ListenerToFrontendPortRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

@dataclass(frozen=True)
class ListenerToSslCertificateRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)

# Relationship: Listener -> Frontend IP
@dataclass(frozen=True)
class ListenerToFrontendIPRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayFrontendIPConfiguration'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('frontend_ip_configuration_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'USES_FRONTEND_IP'
    # Use the new specific property class
    properties: ListenerToFrontendIPRelProperties = ListenerToFrontendIPRelProperties()

# Relationship: Listener -> Frontend Port
@dataclass(frozen=True)
class ListenerToFrontendPortRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewayFrontendPort'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('frontend_port_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'USES_FRONTEND_PORT'
    # Use the new specific property class
    properties: ListenerToFrontendPortRelProperties = ListenerToFrontendPortRelProperties()

# Relationship: Listener -> SSL Certificate
@dataclass(frozen=True)
class ListenerToSslCertificateRel(CartographyRelSchema):
    target_node_label: str = 'AzureApplicationGatewaySslCertificate'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('ssl_certificate_id')},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = 'USES_SSL_CERTIFICATE'
    # Use the new specific property class
    properties: ListenerToSslCertificateRelProperties = ListenerToSslCertificateRelProperties()

# --- END OF FIX ---

@dataclass(frozen=True)
class AzureApplicationGatewayListenerSchema(CartographyNodeSchema):
    label: str = 'AzureApplicationGatewayListener'
    properties: AzureApplicationGatewayListenerProperties = AzureApplicationGatewayListenerProperties()
    sub_resource_relationship: AzureAppGatewayListenerToSubscriptionRel = AzureAppGatewayListenerToSubscriptionRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            AzureAppGatewayListenerToGatewayRel(),
            ListenerToFrontendIPRel(),
            ListenerToFrontendPortRel(),
            ListenerToSslCertificateRel(),
        ],
    )