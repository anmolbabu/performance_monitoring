import ast
import copy
from etcd import EtcdKeyNotFound
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.message import Message
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.objects.system_summary \
    import SystemSummary
from tendrl.performance_monitoring.sds import SDSPlugin
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
from tendrl.performance_monitoring.utils.util import parse_resource_alerts
from tendrl.performance_monitoring.utils.central_store_util \
    import read as etcd_read_key


class GlusterFSPlugin(SDSPlugin):

    name = 'gluster'

    def __init__(self):
        SDSPlugin.__init__(self)
        self.supported_services.extend([
            'tendrl-gluster-integration',
            'glusterd'
        ])

    def configure_monitoring(self, sds_tendrl_context):
        configs = []
        cluster_node_ids = \
            central_store_util.get_cluster_node_ids(
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
                p_conf = copy.deepcopy(plugin_config)
                p_conf['cluster_id'] = \
                    sds_tendrl_context['integration_id']
                p_conf['cluster_name'] = \
                    sds_tendrl_context['cluster_name']
                configs.append({
                    'plugin': "tendrl_%sfs_%s" % (self.name, plugin),
                    'plugin_conf': p_conf,
                    'node_id': node_id,
                    'fqdn': sds_node_context['fqdn']
                })
            configs.append({
                'plugin': "tendrl_%sfs_peer_network_throughput" % (
                    self.name
                ),
                'plugin_conf': {
                    'peer_name': sds_node_context['fqdn']
                },
                'node_id': node_id,
                'fqdn': sds_node_context['fqdn']
            })
        return configs

    def get_brick_status_wise_counts(self, cluster_id, bricks):
        brick_status_wise_counts = {
            'stopped': 0,
            'total': 0,
            pm_consts.WARNING_ALERTS: 0,
            pm_consts.CRITICAL_ALERTS: 0
        }
        for brick_path, brick_det in bricks.iteritems():
            if (
                'status' in brick_det and
                brick_det['status'] == 'Stopped'
            ):
                brick_status_wise_counts['stopped'] = \
                    brick_status_wise_counts['stopped'] + 1
            brick_status_wise_counts['total'] = \
                brick_status_wise_counts['total'] + 1
        crit_alerts, warn_alerts = parse_resource_alerts(
            'brick',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        brick_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ] = len(crit_alerts)
        brick_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = len(warn_alerts)
        return brick_status_wise_counts

    def get_cluster_volume_ids(self, cluster_id):
        volume_ids = []
        try:
            etcd_volume_ids = NS._int.client.read(
                '/clusters/%s/Volumes' % cluster_id
            )
        except EtcdKeyNotFound:
            return volume_ids
        for etcd_volume in etcd_volume_ids.leaves:
            etcd_volume_contents = etcd_volume.key.split('/')
            # /clusters/eb3ce823-70e8-418f-bdc4-d0124ae926f8/Volumes/abce1d94-3918-4faf-bf70-9eee07696da2
            if len(etcd_volume_contents) == 5:
                volume_ids.append(etcd_volume_contents[4])
        return volume_ids

    def get_cluster_volumes(self, cluster_id):
        volumes = {}
        try:
            volume_ids = self.get_cluster_volume_ids(cluster_id)
        except EtcdKeyNotFound:
            return volumes
        for volume_id in volume_ids:
            try:
                volume = etcd_read_key(
                    '/clusters/%s/Volumes/%s' % (
                        cluster_id,
                        volume_id
                    )
                )
                volumes[volume_id] = volume
            except EtcdKeyNotFound:
                continue
        return volumes

    def get_volume_status_wise_counts(self, cluster_id, volumes):
        volume_status_wise_counts = {
            'down': 0,
            'total': 0,
            'degraded': 0,
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0
        }
        # Needs to be tested
        for vol_id, vol_det in volumes.iteritems():
            if 'Started' not in vol_det.get('status'):
                volume_status_wise_counts['down'] = \
                    volume_status_wise_counts['down'] + 1
            volume_status_wise_counts['total'] = \
                volume_status_wise_counts['total'] + 1
        volumes_up_degraded = 0
        try:
            volumes_up_degraded = NS._int.client.read(
                '/clusters/%s/GlobalDetails/volume_up_degraded' % cluster_id
            ).value
        except EtcdKeyNotFound:
            pass
        volume_status_wise_counts['degraded'] = \
            int(volumes_up_degraded or 0)
        crit_alerts, warn_alerts = parse_resource_alerts(
            'volume',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        volume_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ] = len(crit_alerts)
        volume_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = len(warn_alerts)
        return volume_status_wise_counts

    def get_most_used_volumes(self, cluster_name, volumes_det):
        most_used_volumes = []
        v_sort = sorted(
            volumes_det.keys(), key=lambda x: (volumes_det[x]['pcnt_used'])
        )
        v_sort.reverse()
        for volume_id in v_sort:
            vol_det = volumes_det.get(volume_id)
            vol_det['cluster_name'] = cluster_name
            most_used_volumes.append(vol_det)
        return most_used_volumes[:5]

    def get_cluster_bricks(self, cluster_id):
        ret_val = {}
        try:
            etcd_bricks = NS._int.client.read(
                '/clusters/%s/Bricks/all' % cluster_id
            )
        except EtcdKeyNotFound:
            return ret_val
        for etcd_brick in etcd_bricks.leaves:
            try:
                etcd_brick_key_contents = etcd_brick.key.split('/')
                brick = etcd_read_key(
                    '/clusters/%s/Bricks/all/%s' % (
                        cluster_id,
                        etcd_brick_key_contents[5]
                    )
                )
                if 'vol_id' not in brick:
                    continue
                if (
                    'utilization' in brick and
                    'brick_path' in brick
                ):
                    brick['utilization']['vol_name'] = \
                        central_store_util.get_volume_name(
                            cluster_id,
                            brick['vol_id']
                    )
                    brick['utilization']['cluster_name'] = \
                        central_store_util.get_cluster_name(cluster_id)
                    brick['utilization']['brick_path'] = \
                        brick['brick_path']
                    brick['utilization']['hostname'] = \
                        brick['hostname']
                ret_val[etcd_brick_key_contents[5]] = brick
            except EtcdKeyNotFound as ex:
                Event(
                    ExceptionMessage(
                        priority="debug",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "Error fetching details for %s"
                            " brick" % etcd_brick.key,
                            "exception": ex
                        }
                    )
                )
                continue
        return ret_val

    def get_most_used_bricks(self, bricks):
        brick_utilizations = []
        if not bricks:
            return brick_utilizations
        for brick_path, brick_det in bricks.iteritems():
            if 'utilization' in brick_det:
                brick_utilizations.append(brick_det['utilization'])
        brick_utilizations = sorted(
            brick_utilizations,
            key=lambda k: k['used_percent']
        )
        brick_utilizations.reverse()
        return brick_utilizations[:5]

    def get_cluster_summary(self, cluster_id, cluster_name):
        ret_val = {}
        cluster_node_ids = central_store_util.get_cluster_node_ids(cluster_id)
        ret_val['services_count'] = self.get_services_count(
            cluster_node_ids
        )
        volumes = self.get_cluster_volumes(cluster_id)
        bricks = self.get_cluster_bricks(cluster_id)
        ret_val['volume_status_wise_counts'] = \
            self.get_volume_status_wise_counts(
                cluster_id,
                volumes
        )
        ret_val['brick_status_wise_counts'] = \
            self.get_brick_status_wise_counts(
                cluster_id,
                bricks
        )
        ret_val['most_used_volumes'] = self.get_most_used_volumes(
            cluster_name,
            volumes
        )
        ret_val['throughput'] = self.get_cluster_throughput(
            'cluster_network',
            central_store_util.get_cluster_node_contexts(cluster_id),
            cluster_id
        )
        ret_val['most_used_bricks'] = self.get_most_used_bricks(
            bricks
        )
        connection_active = 0
        try:
            connection_active = NS._int.client.read(
                '/clusters/%s/GlobalDetails/connection_active' % cluster_id
            ).value
        except EtcdKeyNotFound:
            pass
        ret_val['connection_active'] = connection_active or 0
        connection_count = 0
        try:
            connection_count = NS._int.client.read(
                '/clusters/%s/GlobalDetails/connection_count' % cluster_id
            ).value
        except EtcdKeyNotFound:
            pass
        ret_val['connection_count'] = connection_count or 0
        return ret_val

    def get_system_client_connection_counts(self, cluster_summaries):
        connection_count = 0
        connection_active = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                connection_count = \
                    connection_count + int(cluster_summary.sds_det.get(
                        'connection_count',
                        0
                    ))
                connection_active = \
                    connection_active + int(cluster_summary.sds_det.get(
                        'connection_active',
                        0
                    ))
        return connection_count, connection_active

    def get_system_brick_status_wise_counts(self, cluster_summaries):
        brick_status_wise_counts = {}
        brick_critical_alerts = 0
        brick_warning_alerts = 0
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
                brick_critical_alerts = \
                    brick_critical_alerts + cluster_brick_count.get(
                        pm_consts.CRITICAL_ALERTS,
                        0
                    )
                brick_warning_alerts = \
                    brick_warning_alerts + cluster_brick_count.get(
                        pm_consts.WARNING_ALERTS,
                        0
                    )
        brick_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            brick_warning_alerts
        brick_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            brick_critical_alerts
        return brick_status_wise_counts

    def get_system_most_used_bricks(self, cluster_summaries):
        brick_utilizations = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                brick_utilizations.extend(
                    cluster_summary.sds_det['most_used_bricks']
                )
        brick_utilizations = sorted(
            brick_utilizations,
            key=lambda k: k['used_percent']
        )
        brick_utilizations.reverse()
        return brick_utilizations[:5]

    def get_system_volume_status_wise_counts(self, cluster_summaries):
        volume_status_wise_counts = {}
        volume_critical_alerts = 0
        volume_warning_alerts = 0
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
                volume_critical_alerts = \
                    volume_critical_alerts + cluster_volume_count.get(
                        pm_consts.CRITICAL_ALERTS,
                        0
                    )
                volume_warning_alerts = \
                    volume_warning_alerts + cluster_volume_count.get(
                        pm_consts.WARNING_ALERTS,
                        0
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

    def get_system_throughput(self, cluster_summaries):
        throughput = 0.0
        cnt = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                throughput = throughput + cluster_summary.sds_det.get(
                    'throughput'
                )
                cnt = cnt + 1
        if cnt > 0:
            throughput = (throughput * 1.0) / (cnt * 1.0)
        NS.time_series_db_manager.get_plugin().push_metrics(
            NS.time_series_db_manager.get_timeseriesnamefromresource(
                sds_type=self.name,
                network_type='cluster_network',
                resource_name=pm_consts.SYSTEM_THROUGHPUT,
                utilization_type=pm_consts.USED
            ),
            throughput
        )
        return throughput

    def compute_system_summary(self, cluster_summaries):
        try:
            connection_count, connection_active = \
                self.get_system_client_connection_counts(cluster_summaries)
            SystemSummary(
                utilization=self.get_system_utilization(cluster_summaries),
                hosts_count=self.get_system_host_status_wise_counts(
                    cluster_summaries
                ),
                cluster_count=self.get_clusters_status_wise_counts(
                    cluster_summaries
                ),
                sds_det={
                    'volume_status_wise_counts': self.get_system_volume_status_wise_counts(
                        cluster_summaries
                    ),
                    'most_used_volumes': self.get_system_max_used_volumes(
                        cluster_summaries
                    ),
                    'services_count': self.get_system_services_count(
                        cluster_summaries
                    ),
                    'brick_status_wise_counts': self.get_system_brick_status_wise_counts(
                        cluster_summaries
                    ),
                    'most_used_bricks': self.get_system_most_used_bricks(
                        cluster_summaries
                    ),
                    'throughput': self.get_system_throughput(
                        cluster_summaries
                    ),
                    'connection_count': connection_count,
                    'connection_active': connection_active
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

    def get_node_brick_status_counts(self, node_id):
        node_name = central_store_util.get_node_name_from_id(node_id)
        ip_indexes = etcd_read_key('/indexes/ip')
        node_ip = ''
        for ip, indexed_node_id in ip_indexes.iteritems():
            if node_id == indexed_node_id:
                node_ip = ip
                break
        brick_status_wise_counts = {
            'stopped': 0,
            'total': 0,
            pm_consts.WARNING_ALERTS: 0,
            pm_consts.CRITICAL_ALERTS: 0
        }
        try:
            cluster_id = central_store_util.get_node_cluster_id(
                node_id
            )
            if cluster_id:
                bricks = self.get_cluster_bricks(cluster_id)
                for brick_path, brick_det in bricks.iteritems():
                    if (
                        brick_det['hostname'] == node_name or
                        brick_det['hostname'] == node_ip
                    ):
                        if (
                            'status' in brick_det and
                            brick_det['status'] == 'Stopped'
                        ):
                            brick_status_wise_counts['stopped'] = \
                                brick_status_wise_counts['stopped'] + 1
                        brick_status_wise_counts['total'] = \
                            brick_status_wise_counts['total'] + 1
            crit_alerts, warn_alerts = parse_resource_alerts(
                'brick',
                pm_consts.CLUSTER,
                cluster_id=cluster_id
            )
            count = 0
            for alert in crit_alerts:
                if alert['node_id'] == node_id:
                    count = count + 1
            brick_status_wise_counts[
                pm_consts.CRITICAL_ALERTS
            ] = count
            count = 0
            for alert in warn_alerts:
                if alert['node_id'] == node_id:
                    count = count + 1
            brick_status_wise_counts[
                pm_consts.WARNING_ALERTS
            ] = count
        except Exception as ex:
            Event(
                Message(
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={"message": "Exception caught fetching node brick"
                                        " status wise counts",
                             "exception": ex
                             }
                )
            )
        return brick_status_wise_counts

    def get_node_summary(self, node_id):
        ret_val = {}
        ret_val['services'] = self.get_node_services_count(node_id)
        ret_val['bricks'] = self.get_node_brick_status_counts(node_id)
        return ret_val
