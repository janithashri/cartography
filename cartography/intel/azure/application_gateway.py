import logging
from typing import Any

import neo4j
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.mgmt.network import NetworkManagementClient
from cartography.client.core.tx import load
from cartography.graph.job import GraphJob
from cartography.util import timeit

from cartography.models.azure.application_gateway.gateway import AzureApplicationGatewaySchema
from cartography.models.azure.application_gateway.frontendipconfiguration import AzureApplicationGatewayFrontendIPConfigurationSchema
from cartography.models.azure.application_gateway.frontendport import AzureApplicationGatewayFrontendPortSchema
from cartography.models.azure.application_gateway.backendpool import AzureApplicationGatewayBackendPoolSchema
from cartography.models.azure.application_gateway.backendsetting import AzureApplicationGatewayBackendSettingSchema
from cartography.models.azure.application_gateway.listener import AzureApplicationGatewayListenerSchema
from cartography.models.azure.application_gateway.requestroutingrule import AzureApplicationGatewayRequestRoutingRuleSchema
from cartography.models.azure.application_gateway.sslcertificate import AzureApplicationGatewaySslCertificateSchema
from cartography.models.azure.application_gateway.healthprobe import AzureApplicationGatewayHealthProbeSchema
from .util.credentials import Credentials

logger = logging.getLogger(__name__)

# ========== Get Functions ==========

@timeit
def get_application_gateways(client: NetworkManagementClient) -> list[dict]:
    """
    Gets Application Gateways using the Network Management Client.
    """
    try:
        return [ag.as_dict() for ag in client.application_gateways.list_all()]
    except ClientAuthenticationError:
        logger.warning(
            "Failed to get Application Gateways due to a client authentication error.",
            exc_info=True,
        )
        raise
    except HttpResponseError:
        logger.warning(
            "Failed to get Application Gateways due to a transient error.",
            exc_info=True,
        )
        return []

# ========== Transform Functions ==========

def transform_application_gateways(gateways: list[dict]) -> list[dict]:
    transformed = []
    for ag in gateways:
        transformed.append({
            'id': ag.get('id'),
            'name': ag.get('name'),
            'location': ag.get('location'),
            'sku_name': ag.get('sku', {}).get('name'),
            'provisioning_state': ag.get('properties', {}).get('provisioningState'),
        })
    return transformed

def transform_frontend_ip_configurations(gateway: dict) -> list[dict]:
    transformed = []
    for config in gateway.get('frontend_ip_configurations', []):
        transformed.append({
            'id': config.get('id'),
            'name': config.get('name'),
            'private_ip_address': config.get('properties', {}).get('private_ip_address'),
            'public_ip_address_id': config.get('properties', {}).get('public_ip_address', {}).get('id'),
        })
    return transformed

def transform_frontend_ports(gateway: dict) -> list[dict]:
    transformed = []
    for port in gateway.get('frontend_ports', []):
        transformed.append({
            'id': port.get('id'),
            'name': port.get('name'),
            'port': port.get('properties', {}).get('port'),
        })
    return transformed

def transform_backend_pools(gateway: dict) -> list[dict]:
    transformed = []
    for pool in gateway.get('backend_address_pools', []):
        transformed.append({
            'id': pool.get('id'),
            'name': pool.get('name'),
        })
    return transformed

def transform_backend_settings(gateway: dict) -> list[dict]:
    transformed = []
    print("\n--- DEBUG: RAW DATA FOR BACKEND SETTINGS ---")
    # Note: The API name is backend_http_settings_collection
    for setting in gateway.get('backend_http_settings_collection', []):
        print(setting)
        properties = setting.get('properties', {})
        transformed.append({
            'id': setting.get('id'),
            'name': setting.get('name'),
            'port': properties.get('port'),
            'protocol': properties.get('protocol'),
            'cookie_based_affinity': properties.get('cookieBasedAffinity'),
            'probe_id': properties.get('probe', {}).get('id'), 
        })
    return transformed

def transform_listeners(gateway: dict) -> list[dict]:
    transformed = []
    # Note: The API name is http_listeners
    print("\n--- DEBUG: RAW DATA FOR LISTENERS ---")
    for listener in gateway.get('http_listeners', []):
        print(listener)
        properties = listener.get('properties', {})
        transformed.append({
            'id': listener.get('id'),
            'name': listener.get('name'),
            'protocol': properties.get('protocol'),
            'frontend_ip_configuration_id': properties.get('frontendIPConfiguration', {}).get('id'),
            'frontend_port_id': properties.get('frontendPort', {}).get('id'),
            'ssl_certificate_id': properties.get('sslCertificate', {}).get('id'),
        })
    return transformed

def transform_request_routing_rules(gateway: dict) -> list[dict]:
    transformed = []
    print("\n--- DEBUG: RAW DATA FOR ROUTING RULES ---")
    for rule in gateway.get('request_routing_rules', []):
        print(rule)
        properties = rule.get('properties', {})
        transformed.append({
            'id': rule.get('id'),
            'name': rule.get('name'),
            'rule_type': properties.get('ruleType'),
            'priority': properties.get('priority'),
            'listener_id': properties.get('httpListener', {}).get('id'),
            'backend_pool_id': properties.get('backendAddressPool', {}).get('id'),
            'backend_setting_id': properties.get('backendHttpSettings', {}).get('id'),
        })
    return transformed

def transform_ssl_certificates(gateway: dict) -> list[dict]:
    transformed = []
    for cert in gateway.get('sslCertificates', []):
        transformed.append({
            'id': cert.get('id'),
            'name': cert.get('name'),
            'key_vault_secret_id': cert.get('properties', {}).get('keyVaultSecretId'),
        })
    return transformed

def transform_health_probes(gateway: dict) -> list[dict]:
    transformed = []
    for probe in gateway.get('probes', []):
        properties = probe.get('properties', {})
        transformed.append({
            'id': probe.get('id'),
            'name': probe.get('name'),
            'protocol': properties.get('protocol'),
            'path': properties.get('path'),
            'interval': properties.get('interval'),
            'timeout': properties.get('timeout'),
            'unhealthy_threshold': properties.get('unhealthyThreshold'),
        })
    return transformed

# ========== Main Sync Function ==========

@timeit
def sync(
    neo4j_session: neo4j.Session,
    credentials: Credentials,
    subscription_id: str,
    update_tag: int,
    common_job_parameters: dict,
) -> None:
    """
    The main sync function for Azure Application Gateways.
    """
    logger.info(f"Syncing Azure Application Gateways for subscription '{subscription_id}'.")
    client = NetworkManagementClient(credentials.credential, subscription_id)

    # 1. GET & LOAD GATEWAYS
    gateways_raw = get_application_gateways(client)
    if not gateways_raw:
        return
    gateways = transform_application_gateways(gateways_raw)
    load(
        neo4j_session, AzureApplicationGatewaySchema(), gateways,
        lastupdated=update_tag, AZURE_SUBSCRIPTION_ID=subscription_id,
    )

    # 2. GET (implicitly done via raw data), TRANSFORM, & LOAD CHILD COMPONENTS
    for ag_raw in gateways_raw:
        ag_id = ag_raw['id']

        frontend_ips = transform_frontend_ip_configurations(ag_raw)
        if frontend_ips:
            load(
                neo4j_session, AzureApplicationGatewayFrontendIPConfigurationSchema(), frontend_ips,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        frontend_ports = transform_frontend_ports(ag_raw)
        if frontend_ports:
            load(
                neo4j_session, AzureApplicationGatewayFrontendPortSchema(), frontend_ports,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        backend_pools = transform_backend_pools(ag_raw)
        if backend_pools:
            load(
                neo4j_session, AzureApplicationGatewayBackendPoolSchema(), backend_pools,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        # Order matters: Load probes before settings that use them
        health_probes = transform_health_probes(ag_raw)
        if health_probes:
            load(
                neo4j_session, AzureApplicationGatewayHealthProbeSchema(), health_probes,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        backend_settings = transform_backend_settings(ag_raw)
        if backend_settings:
            load(
                neo4j_session, AzureApplicationGatewayBackendSettingSchema(), backend_settings,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        # Order matters: Load certs before listeners that use them
        ssl_certs = transform_ssl_certificates(ag_raw)
        if ssl_certs:
            load(
                neo4j_session, AzureApplicationGatewaySslCertificateSchema(), ssl_certs,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        listeners = transform_listeners(ag_raw)
        if listeners:
            load(
                neo4j_session, AzureApplicationGatewayListenerSchema(), listeners,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

        rules = transform_request_routing_rules(ag_raw)
        if rules:
            load(
                neo4j_session, AzureApplicationGatewayRequestRoutingRuleSchema(), rules,
                lastupdated=update_tag, GATEWAY_ID=ag_id, AZURE_SUBSCRIPTION_ID=subscription_id,
            )

    # 3. CLEANUP (Run cleanup for all node types)
    GraphJob.from_node_schema(AzureApplicationGatewaySchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayFrontendIPConfigurationSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayFrontendPortSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayBackendPoolSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayBackendSettingSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayListenerSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayRequestRoutingRuleSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewaySslCertificateSchema(), common_job_parameters).run(neo4j_session)
    GraphJob.from_node_schema(AzureApplicationGatewayHealthProbeSchema(), common_job_parameters).run(neo4j_session)

