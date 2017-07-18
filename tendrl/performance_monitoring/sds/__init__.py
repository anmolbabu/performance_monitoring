from abc import abstractmethod
import ast
from etcd import EtcdException
from etcd import EtcdKeyNotFound
import importlib
import inspect
import os
import six
import urllib3
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.message import Message
from tendrl.performance_monitoring.utils.util import parse_resource_alerts
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
from tendrl.performance_monitoring.utils.util import get_latest_node_stat
from tendrl.performance_monitoring.utils.util \
    import list_modules_in_package_path


class NoSDSPluginException(Exception):
    pass


class PluginMount(type):

    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'plugins'):
            cls.plugins = []
        else:
            cls.register_plugin(cls)

    def register_plugin(cls, plugin):
        instance = plugin()
        cls.plugins.append(instance)


@six.add_metaclass(PluginMount)
class SDSPlugin(object):
    name = ''

    def __init__(self):
        self.supported_services = [
            'tendrl-node-agent',
            'etcd'
        ]

    @abstractmethod
    def configure_monitoring(self, sds_tendrl_context):
        raise NotImplementedError(
            "The plugins overriding SDSPlugin should mandatorily override this"
        )

    @abstractmethod
    def get_cluster_summary(self, integration_id, cluster_name):
        raise NotImplementedError(
            "The plugins overriding SDSPlugin should mandatorily override this"
        )

    @abstractmethod
    def compute_system_summary(self, cluster_summaries):
        raise NotImplementedError(
            "The plugins overriding SDSPlugin should mandatorily override this"
        )

    def get_clusters_status_wise_counts(self, cluster_summaries):
        clusters_status_wise_counts = {
            'status': {
                'total': 0
            },
            'near_full': 0,
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0
        }
        cluster_alerts = []
        for cluster_summary in cluster_summaries:
            cluster_tendrl_context = {}
            cluster_status = {}
            sds_name = central_store_util.get_cluster_sds_name(
                cluster_summary.integration_id
            )
            try:
                cluster_tendrl_context = central_store_util.read(
                    '/clusters/%s/TendrlContext' % cluster_summary.integration_id
                )
                cluster_status = central_store_util.read(
                    '/clusters/%s/GlobalDetails' % cluster_summary.integration_id
                )
                cluster_status = cluster_status.get('status')
            except EtcdKeyNotFound:
                return clusters_status_wise_counts
            if (
                self.name in
                    cluster_tendrl_context.get('sds_name')
            ):
                if cluster_status:
                    if (
                        cluster_status not in
                            clusters_status_wise_counts['status']
                    ):
                        clusters_status_wise_counts['status'][
                            cluster_status
                        ] = 1
                    else:
                        clusters_status_wise_counts['status'][
                            cluster_status
                        ] = \
                            clusters_status_wise_counts['status'][
                                cluster_status
                        ] + 1
                    clusters_status_wise_counts['status']['total'] = \
                        clusters_status_wise_counts['status']['total'] + 1
                cluster_critical_alerts, cluster_warning_alerts = \
                    parse_resource_alerts(
                        None,
                        pm_consts.CLUSTER,
                        integration_id=cluster_summary.integration_id
                    )
                cluster_alerts.extend(cluster_critical_alerts)
                cluster_alerts.extend(cluster_warning_alerts)
                clusters_status_wise_counts[
                    pm_consts.CRITICAL_ALERTS
                ] = clusters_status_wise_counts[
                    pm_consts.CRITICAL_ALERTS
                ] + len(cluster_critical_alerts)
                clusters_status_wise_counts[
                    pm_consts.WARNING_ALERTS
                ] = clusters_status_wise_counts[
                    pm_consts.WARNING_ALERTS
                ] + len(cluster_warning_alerts)
        for cluster_alert in cluster_alerts:
            if (
                cluster_alert['severity'] == pm_consts.CRITICAL and
                cluster_alert['resource'] == 'cluster_utilization'
            ):
                clusters_status_wise_counts['near_full'] = \
                    clusters_status_wise_counts.get('near_full', 0) + 1
        return clusters_status_wise_counts

    def get_system_utilization(self, cluster_summaries):
        net_utilization = {
            'total': 0,
            'used': 0,
            'percent_used': 0
        }
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                net_utilization['total'] = \
                    net_utilization['total'] + int(
                        cluster_summary.utilization.get(
                            'total', 0
                        )
                )
                net_utilization['used'] = \
                    net_utilization['used'] + int(
                        cluster_summary.utilization.get(
                            'used', 0
                        )
                )
                net_utilization['percent_used'] = 0
                if net_utilization['total'] > 0:
                    net_utilization['percent_used'] = (
                        net_utilization['used'] * 100
                    ) / (
                        net_utilization['total'] * 1.0
                    )
        if net_utilization['total']:
        # Push the computed system utilization to time-series db
            NS.time_series_db_manager.get_plugin().push_metrics(
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    sds_type=self.name,
                    utilization_type=pm_consts.TOTAL,
                    resource_name=pm_consts.SYSTEM_UTILIZATION
                ),
                net_utilization[pm_consts.TOTAL]
            )
            NS.time_series_db_manager.get_plugin().push_metrics(
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    sds_type=self.name,
                    utilization_type=pm_consts.USED,
                    resource_name=pm_consts.SYSTEM_UTILIZATION
                ),
                net_utilization[pm_consts.USED]
            )
            NS.time_series_db_manager.get_plugin().push_metrics(
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    sds_type=self.name,
                    utilization_type=pm_consts.PERCENT_USED,
                    resource_name=pm_consts.SYSTEM_UTILIZATION
                ),
                net_utilization[pm_consts.PERCENT_USED]
            )
        return net_utilization

    def get_system_host_status_wise_counts(self, cluster_summaries):
        status_wise_count = {
            'total': 0,
            'down': 0,
            'crit_alert_count': 0,
            'warn_alert_count': 0
        }
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                for status, counter in cluster_summary.hosts_count.iteritems():
                    status_wise_count[status] = \
                        status_wise_count.get(status, 0) + int(counter)
        return status_wise_count

    def get_node_services_count(self, node_id):
        services = {}
        try:
            services = central_store_util.read('nodes/%s/Services' % node_id)
        except EtcdKeyNotFound as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": 'Failed to fetch services of '
                        'node %s' % node_id,
                        "exception": ex
                    }
                )
            )
        return services

    def get_services_count(self, cluster_node_ids):
        node_service_counts = {}
        for node_id in cluster_node_ids:
            try:
                services = central_store_util.read(
                    'nodes/%s/Services' % node_id
                )
            except EtcdKeyNotFound as ex:
                Event(
                    ExceptionMessage(
                        priority="debug",
                        publisher=NS.publisher_id,
                        payload={
                            "message": 'Failed to fetch services of '
                            'node %s' % node_id,
                            "exception": ex
                        }
                    )
                )
                continue
            for service_name, service_det in services.iteritems():
                try:
                    if service_name in self.supported_services:
                        if service_name not in node_service_counts:
                            service_counter = {'running': 0, 'not_running': 0}
                        else:
                            service_counter = node_service_counts[service_name]
                        if service_det['exists'] == 'True':
                            if service_det['running'] == 'True':
                                service_counter['running'] = \
                                    service_counter['running'] + 1
                            else:
                                service_counter['not_running'] = \
                                    service_counter['not_running'] + 1
                            node_service_counts[service_name] = service_counter
                except (ValueError, AttributeError, KeyError) as ex:
                    Event(
                        ExceptionMessage(
                            priority="debug",
                            publisher=NS.publisher_id,
                            payload={
                                "message": 'Failed to parse services of '
                                'node %s' % node_id,
                                "exception": ex
                            }
                        )
                    )
                    continue
        return node_service_counts

    def get_system_services_count(self, cluster_summaries):
        system_services_count = {}
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                services_count = cluster_summary.sds_det.get('services_count')
                if isinstance(services_count, basestring):
                    services_count = ast.literal_eval(
                        services_count.encode('ascii', 'ignore')
                    )
                for service_name, service_status_counter in \
                        services_count.iteritems():
                    service_counter = {}
                    for service_status, counter in \
                            service_status_counter.iteritems():
                        service_counter[service_status] = \
                            service_counter.get(
                                service_status, 0
                        ) + int(counter)
                    system_services_count[service_name] = service_counter
        return system_services_count

    def get_cluster_throughput(self, nw_type, cluster_nodes, integration_id):
        throughput = 0.0
        cnt = 0
        for node_id, node_context in cluster_nodes.iteritems():
            try:
                node_name = node_context.get(
                    'fqdn',
                    ''
                )
                entity_name, metric_name = \
                    NS.time_series_db_manager.get_timeseriesnamefromresource(
                        node_name=node_name,
                        network_type=nw_type,
                        resource_name=pm_consts.NODE_THROUGHPUT,
                        utilization_type=pm_consts.USED
                    ).split(
                        "%s%s" % (
                            node_name,
                            NS.time_series_db_manager.get_plugin(
                            ).get_delimeter()
                        ),
                        1
                    )
                curr_throughput = get_latest_node_stat(
                    node_id,
                    metric_name
                )
                throughput = throughput + curr_throughput
                cnt = cnt + 1
            except (
                ValueError,
                urllib3.exceptions.HTTPError,
                TendrlPerformanceMonitoringException
            ):
                continue
        if cnt > 0:
            throughput = (throughput * 1.0) / (cnt * 1.0)
        NS.time_series_db_manager.get_plugin().push_metrics(
            NS.time_series_db_manager.get_timeseriesnamefromresource(
                integration_id=integration_id,
                network_type=nw_type,
                resource_name=pm_consts.CLUSTER_THROUGHPUT,
                utilization_type=pm_consts.USED
            ),
            throughput
        )
        return throughput

    def get_node_summary(self, node_id):
        raise NotImplementedError(
            "The plugins overriding SDSPlugin should mandatorily override this"
        )


class SDSMonitoringManager(object):
    def load_sds_plugins(self):
        path = os.path.dirname(os.path.abspath(__file__))
        pkg = 'tendrl.performance_monitoring.sds'
        sds_plugins = list_modules_in_package_path(path, pkg)
        for name, sds_fqdn in sds_plugins:
            mod = importlib.import_module(sds_fqdn)
            clsmembers = inspect.getmembers(mod, inspect.isclass)
            for name, cls in clsmembers:
                if issubclass(cls, SDSPlugin):
                    if cls.name:
                        self.supported_sds.append(cls.name)

    def __init__(self):
        self.supported_sds = []
        self.load_sds_plugins()

    def get_cluster_summary(self, integration_id, cluster_name):
        sds_name = central_store_util.get_cluster_sds_name(integration_id)
        for plugin in SDSPlugin.plugins:
            if plugin.name == sds_name:
                return plugin.get_cluster_summary(integration_id, cluster_name)

    def compute_system_summary(self, cluster_summaries):
        for plugin in SDSPlugin.plugins:
            plugin.compute_system_summary(cluster_summaries)

    def configure_monitoring(self, integration_id):
        try:
            sds_tendrl_context = central_store_util.read(
                'clusters/%s/TendrlContext' % integration_id
            )
        except EtcdKeyNotFound:
            return None
        except EtcdException as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": 'Failed to configure monitoring for '
                        'cluster %s as tendrl context could '
                        'not be fetched.' % integration_id,
                        "exception": ex
                    }
                )
            )
            return
        for plugin in SDSPlugin.plugins:
            if plugin.name == sds_tendrl_context.get('sds_name', ''):
                return plugin.configure_monitoring(sds_tendrl_context)
        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": 'No plugin defined for %s. Hence cannot '
                    'configure it' % sds_tendrl_context['sds_name']
                }
            )
        )
        return None

    def get_node_summary(self, node_id):
        ret_val = {}
        sds_name = central_store_util.get_node_sds_name(node_id)
        if sds_name == "":
            return ret_val
        for plugin in SDSPlugin.plugins:
            if plugin.name == sds_name:
                return plugin.get_node_summary(node_id)
