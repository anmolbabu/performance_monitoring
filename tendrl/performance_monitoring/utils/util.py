from etcd import EtcdException
import json
import math
import pkgutil
import re
import urllib3
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.objects.job import Job
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.objects.node_monitoring_plugin import \
    NodeMonitoringPlugin
import uuid
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util


def list_modules_in_package_path(package_path, prefix):
    modules = []
    path_to_walk = [(package_path, prefix)]
    while len(path_to_walk) > 0:
        curr_path, curr_prefix = path_to_walk.pop()
        for importer, name, ispkg in pkgutil.walk_packages(
            path=[curr_path]
        ):
            if ispkg:
                path_to_walk.append(
                    (
                        '%s/%s/' % (curr_path, name),
                        '%s.%s' % (curr_prefix, name)
                    )
                )
            else:
                modules.append((name, '%s.%s' % (curr_prefix, name)))
    return modules


def initiate_config_generation(node_det):
    try:
        plugin = NodeMonitoringPlugin(
            plugin_name=node_det['plugin'],
            node_id=node_det.get('node_id')
        )
        if plugin.exists():
            # More powers like fixed retrials can be added here.This is common
            # point through which all monitoring plugin configuration jobs land
            # into etcd and hence any action here is reflected to all of them.
            return
        job_params = {
            'tags': ["tendrl/node_%s" % node_det.get('node_id')],
            "run": 'node_monitoring.flows.ConfigureCollectd',
            'type': 'monitoring',
            "parameters": {
                'plugin_name': node_det['plugin'],
                'plugin_conf_params': json.dumps(
                    node_det['plugin_conf']
                ).encode('utf-8'),
                'Node.fqdn': node_det['fqdn'],
                'Service.name': 'collectd',
            },
        }
        job_id = str(uuid.uuid4())
        Job(
            job_id=job_id,
            status='new',
            payload=job_params,
        ).save()
        NodeMonitoringPlugin(
            plugin_name=node_det['plugin'],
            node_id=node_det.get('node_id'),
            job_id=job_id
        ).save(update=False)
    except (
        EtcdException,
        AttributeError
    ) as ex:
        Event(
            ExceptionMessage(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": 'Failed to intiate monitoring configuration for'
                    ' plugin %s on %s with parameters %s.' % (
                        node_det['plugin'],
                        node_det['fqdn'],
                        json.dumps(node_det['plugin_conf'])
                    ),
                    "exception": ex
                }
            )
        )


def parse_resource_alerts(resource_type, resource_classification, **kwargs):
    alerts = {}
    if resource_classification == pm_consts.NODE:
        alerts = central_store_util.get_node_alerts(
            **kwargs
        )
    if resource_classification == pm_consts.CLUSTER:
        alerts = central_store_util.get_cluster_alerts(
            **kwargs
        )
    critical_alerts = []
    warning_alerts = []
    for alert in alerts:
        if alert['acked'].lower() == "true":
            continue
        if resource_type:
            for alert_type in pm_consts.SUPPORTED_ALERT_TYPES:
                if alert['resource'] == '%s_%s' % (resource_type, alert_type):
                    if alert['severity'] == pm_consts.CRITICAL:
                        critical_alerts.append(alert)
                    if alert['severity'] == pm_consts.WARNING:
                        warning_alerts.append(alert)
        else:
            if alert['severity'] == pm_consts.CRITICAL:
                critical_alerts.append(alert)
            if alert['severity'] == pm_consts.WARNING:
                warning_alerts.append(alert)
    return critical_alerts, warning_alerts


''' Get latest stats of resources matching wild cards in param resource'''


def get_latest_stats(node, resource):
    try:
        node_name = central_store_util.get_node_name_from_id(node)
        stats = NS.time_series_db_manager.get_plugin().get_metric_stats(
            node_name,
            resource,
            'latest'
        )
        if stats == "[]" or not stats:
            raise TendrlPerformanceMonitoringException(
                'Stats not yet available in time series db'
            )
        return re.findall('Current:(.+?)Max', stats)
    except (
        ValueError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        Event(
            ExceptionMessage(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": 'Failed to get latest stats of %s of '
                    'node %s for node summary.' % (
                        resource,
                        node
                    ),
                    "exception": ex
                }
            )
        )
        raise ex


def get_latest_node_stat(node, resource):
    try:
        node_name = central_store_util.get_node_name_from_id(
            node
        )
        return get_latest_stat(node_name, resource)
    except (
        ValueError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        raise ex


''' Get latest stats of resource as in param resource'''


def get_latest_stat(node, resource):
    try:
        stats = NS.time_series_db_manager.get_plugin().get_metric_stats(
            node,
            resource,
            'latest'
        )
        if stats == "[]" or not stats:
            raise TendrlPerformanceMonitoringException(
                'Stats not yet available in time series db'
            )
        stat = re.search('Current:(.+?)Max', stats)
        if not stat:
            raise TendrlPerformanceMonitoringException(
                'Failed to get latest stat of %s of node %s for summary'
                'Error: Current utilization not found' % (
                    resource,
                    node
                )
            )
        stat = re.search('Current:(.+?)Max', stats).group(1)
        if math.isnan(float(stat)):
            raise TendrlPerformanceMonitoringException(
                'Received nan for utilization %s of %s' % (
                    resource,
                    node
                )
            )
        return float(stat)
    except (
        ValueError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        Event(
            ExceptionMessage(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": 'Failed to get latest stat of %s of '
                    'node %s for node summary.' % (
                        resource,
                        node
                    ),
                    "exception": ex
                }
            )
        )
        raise ex
