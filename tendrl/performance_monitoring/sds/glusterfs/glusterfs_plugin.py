import ast

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.objects.system_summary \
    import SystemSummary
from tendrl.performance_monitoring.sds import SDSPlugin
from tendrl.performance_monitoring.utils import parse_resource_alerts
from tendrl.performance_monitoring.utils import read as etcd_read_key


class GlusterFSPlugin(SDSPlugin):

    name = 'gluster'

    def __init__(self):
        SDSPlugin.__init__(self)
        self.supported_services.extend([
            'tendrl-gluster-integration',
            'glusterd'
        ])
        self.configured_nodes = {}

    def configure_monitoring(self, sds_tendrl_context):
        configs = []
        cluster_node_ids = \
            NS.central_store_thread.get_cluster_node_ids(
                sds_tendrl_context['integration_id']
            )
        for node_id in cluster_node_ids:
            sds_node_context = etcd_read_key(
                '/clusters/%s/nodes/%s/NodeContext' % (
                    sds_tendrl_context['integration_id'],
                    node_id
                )
            )
            config = NS.performance_monitoring.config.data['thresholds']
            if isinstance(config, basestring):
                config = ast.literal_eval(config.encode('ascii', 'ignore'))
            for plugin, plugin_config in config[self.name].iteritems():
                if isinstance(plugin_config, basestring):
                    plugin_config = ast.literal_eval(
                        plugin_config.encode('ascii', 'ignore')
                    )
                is_configured = True
                if node_id not in self.configured_nodes:
                    self.configured_nodes[node_id] = [plugin]
                    is_configured = False
                if plugin not in self.configured_nodes[node_id]:
                    node_plugins = self.configured_nodes[node_id]
                    node_plugins.append(plugin)
                    is_configured = False
                if not is_configured:
                    plugin_config['cluster_id'] = \
                        sds_tendrl_context['integration_id']
                    configs.append({
                        'plugin': "%sfs_%s" % (self.name, plugin),
                        'plugin_conf': plugin_config,
                        'node_id': node_id,
                        'fqdn': sds_node_context['fqdn']
                    })
        return configs

    def get_brick_status_wise_counts(self, volumes_det, cluster_id):
        brick_status_wise_counts = {'stopped': 0, 'total': 0}
        for volume_id, volume_det in volumes_det.iteritems():
            for brick_path, brick_det in volume_det['Bricks'].iteritems():
                if brick_det['status'] == 'Stopped':
                    brick_status_wise_counts['stopped'] = \
                        brick_status_wise_counts['stopped'] + 1
                brick_status_wise_counts['total'] = \
                    brick_status_wise_counts['total'] + 1
        brick_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ], brick_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = parse_resource_alerts(
            'brick',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        return brick_status_wise_counts

    def get_volume_status_wise_counts(self, volumes_det, cluster_id):
        volume_status_wise_counts = {'down': 0, 'total': 0}
        # Needs to be tested
        for vol_id, vol_det in volumes_det.iteritems():
            if 'Started' not in vol_det.get('status'):
                volume_status_wise_counts['down'] = \
                    volume_status_wise_counts['down'] + 1
            volume_status_wise_counts['total'] = \
                volume_status_wise_counts['total'] + 1
        volume_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ], volume_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = parse_resource_alerts(
            'volume',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        return volume_status_wise_counts

    def get_most_used_volumes(self, cluster_det):
        volumes_det = cluster_det.get('Volumes', {})
        # Needs to be tested
        most_used_volumes = []
        v_sort = sorted(
            volumes_det.keys(), key=lambda x: (volumes_det[x]['pcnt_used'])
        )
        v_sort.reverse()
        for volume_id in v_sort:
            vol_det = volumes_det.get(volume_id)
            vol_det['cluster_name'] = cluster_det.get(
                'TendrlContext',
                {}
            ).get('cluster_name', '')
            most_used_volumes.append(vol_det)
        return most_used_volumes[:5]

    def get_cluster_summary(self, cluster_id, cluster_det):
        ret_val = {}
        ret_val['services_count'] = self.get_services_count(
            cluster_det
        )
        ret_val['volume_status_wise_counts'] = \
            self.get_volume_status_wise_counts(
                cluster_det.get('Volumes', {}),
                cluster_id
        )
        ret_val['brick_status_wise_counts'] = \
            self.get_brick_status_wise_counts(
                cluster_det.get('Volumes', {}),
                cluster_id
        )
        ret_val['most_used_volumes'] = self.get_most_used_volumes(
            cluster_det
        )
        return ret_val

    def get_system_brick_status_wise_counts(self, cluster_summaries):
        brick_status_wise_counts = {}
        brick_critical_alerts = []
        brick_warning_alerts = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_brick_count = cluster_summary.sds_det.get(
                    'brick_status_wise_counts', {}
                )
                if isinstance(cluster_brick_count, unicode):
                    cluster_brick_count = ast.literal_eval(
                        cluster_brick_count.encode('ascii', 'replace')
                    )
                for status, count in cluster_brick_count.iteritems():
                    if isinstance(count, int):
                        brick_status_wise_counts[status] = \
                            brick_status_wise_counts.get(status, 0) + \
                            int(count)
                brick_critical_alerts.extend(
                    cluster_brick_count.get(
                        pm_consts.CRITICAL_ALERTS
                    )
                )
                brick_warning_alerts.extend(
                    cluster_brick_count.get(
                        pm_consts.WARNING_ALERTS
                    )
                )
        brick_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            brick_warning_alerts
        brick_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            brick_critical_alerts
        return brick_status_wise_counts

    def get_system_volume_status_wise_counts(self, cluster_summaries):
        volume_status_wise_counts = {}
        volume_critical_alerts = []
        volume_warning_alerts = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_volume_count = cluster_summary.sds_det.get(
                    'volume_status_wise_counts', {}
                )
                if isinstance(cluster_volume_count, unicode):
                    cluster_volume_count = ast.literal_eval(
                        cluster_volume_count.encode('ascii', 'replace')
                    )
                for status, count in cluster_volume_count.iteritems():
                    if isinstance(count, int):
                        volume_status_wise_counts[status] = \
                            volume_status_wise_counts.get(status, 0) + \
                            int(count)
                volume_critical_alerts.extend(
                    cluster_volume_count.get(
                        pm_consts.CRITICAL_ALERTS
                    )
                )
                volume_warning_alerts.extend(
                    cluster_volume_count.get(
                        pm_consts.WARNING_ALERTS
                    )
                )
        volume_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            volume_warning_alerts
        volume_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            volume_critical_alerts
        return volume_status_wise_counts

    def get_system_max_used_volumes(self, cluster_summaries):
        most_used_volumes = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_most_used_volumes = \
                    cluster_summary.sds_det.get('most_used_volumes', {})
                if isinstance(cluster_most_used_volumes, basestring):
                    cluster_most_used_volumes = ast.literal_eval(
                        cluster_most_used_volumes.encode(
                            'ascii', 'ignore'
                        )
                    )
                for volume in cluster_most_used_volumes:
                    if isinstance(volume, unicode):
                        volume = volume.encode('ascii', 'ignore')
                        volume = ast.literal_eval(volume)
                    most_used_volumes.append(volume)
        most_used_volumes = \
            sorted(most_used_volumes, key=lambda k: k['pcnt_used'])
        most_used_volumes.reverse()
        return most_used_volumes[:5]

    def compute_system_summary(self, cluster_summaries, clusters):
        try:
            SystemSummary(
                utilization=self.get_system_utilization(cluster_summaries),
                hosts_count=self.get_system_host_status_wise_counts(
                    cluster_summaries
                ),
                cluster_count=self.get_clusters_status_wise_counts(clusters),
                sds_det={
                    'volume_counts': self.get_system_volume_status_wise_counts(
                        cluster_summaries
                    ),
                    'most_used_volumes': self.get_system_max_used_volumes(
                        cluster_summaries
                    ),
                    'services_count': self.get_system_services_count(
                        cluster_summaries
                    ),
                    'brick_counts': self.get_system_brick_status_wise_counts(
                        cluster_summaries
                    )
                },
                sds_type=self.name
            ).save(update=False)
        except Exception as ex:
            Event(
                ExceptionMessage(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={"message": "Exception caught computing system "
                                        "summary.",
                             "exception": ex
                             }
                )
            )
