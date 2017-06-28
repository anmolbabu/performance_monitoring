import ast
import copy
from etcd import EtcdKeyNotFound
from etcd import EtcdException
import json
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.objects.system_summary \
    import SystemSummary
from tendrl.performance_monitoring.sds import SDSPlugin
from tendrl.performance_monitoring.sds.ceph.pg_utils \
    import _calculate_pg_counters
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
from tendrl.performance_monitoring.utils.util import parse_resource_alerts
import tendrl.performance_monitoring.utils.threshold_utils as threshold_utils
from tendrl.performance_monitoring.utils.central_store_util \
    import read as etcd_read_key


class CephUtilizationTypes(threshold_utils.UtilizationTypes):
    POOL_UTILIZATION = 'pool_utilization'
    OSD_UTILIZATION = 'osd_utilization'
    RBD_UTILIZATION = 'rbd_utilization'


class CephPlugin(SDSPlugin):

    name = 'ceph'

    def __init__(self):
        SDSPlugin.__init__(self)
        self.supported_services.extend([
            'tendrl-ceph-integration',
            'ceph-mon',
            'ceph-osd'
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
            if 'mon' in sds_node_context['tags']:
                config = threshold_utils.get_thresholds(
                    integration_id=sds_tendrl_context['integration_id'],
                    sds_name=sds_tendrl_context['sds_name']
                )
                if isinstance(config, basestring):
                    config = ast.literal_eval(
                        config.encode('ascii', 'ignore')
                    )
                for plugin, plugin_config in config.iteritems():
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
                        'plugin': "tendrl_%s_%s" % (self.name, plugin),
                        'plugin_conf': p_conf,
                        'node_id': node_id,
                        'fqdn': sds_node_context['fqdn']
                    })
                plugin_config = {
                    'cluster_id': sds_tendrl_context['integration_id'],
                    'cluster_name': sds_tendrl_context['cluster_name']
                }
                configs.append({
                    'plugin': "tendrl_ceph_cluster_iops",
                    'plugin_conf': plugin_config,
                    'node_id': node_id,
                    'fqdn': sds_node_context['fqdn']
                })
            plugin_config = {}
            plugin_config['cluster_network'] = ' '.join(
                self.get_nw_node_interfaces(
                    node_id,
                    'cluster_network',
                    sds_tendrl_context['integration_id']
                )
            )
            plugin_config['public_network'] = ' '.join(
                self.get_nw_node_interfaces(
                    node_id,
                    'public_network',
                    sds_tendrl_context['integration_id']
                )
            )
            configs.append({
                'plugin': "tendrl_%s_node_network_throughput" % (
                    self.name
                ),
                'plugin_conf': plugin_config,
                'node_id': node_id,
                'fqdn': sds_node_context['fqdn']
            })
        return configs

    def get_nw_node_interfaces(self, node_id, nw_type, cluster_id):
        nw_node_interfaces = []
        try:
            nw_subnet = etcd_read_key(
                '/clusters/%s/maps/config/data' % (
                    cluster_id
                )
            )
            nw_subnet = json.loads(nw_subnet.get('data', '{}'))
            nw_subnet = nw_subnet.get(nw_type, '')
            if nw_subnet:
                nw_subnet = nw_subnet.replace('/', '_')
                networks = etcd_read_key(
                    '/networks/%s/%s' % (nw_subnet, node_id)
                )
                for interface_id, interface_det in networks.iteritems():
                    nw_node_interfaces.append(
                        interface_det.get('interface')
                    )
        except (
            EtcdKeyNotFound,
            ValueError,
            TypeError,
            AttributeError
        ):
            pass
        except TendrlPerformanceMonitoringException as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Error fetching %s n/w info for node "
                        "%s" % (nw_type, node_id),
                        "exception": ex
                    }
                )
            )
        return nw_node_interfaces

    def get_cluster_pool_ids(self, cluster_id):
        pools = []
        try:
            etcd_pools = central_store_util.read_key(
                '/clusters/%s/Pools/' % cluster_id
            )
            for etcd_pool in etcd_pools.leaves:
                # /clusters/a88ada59-f52b-4608-9311-96cccfbbbf6a/Pools/0
                pool_key_contents = etcd_pool.key.split('/')
                if len(pool_key_contents) == 5:
                    pools.append(pool_key_contents[4])
        except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
            pass
        return pools

    def get_cluster_pools(self, cluster_id):
        pools = {}
        try:
            pool_ids = self.get_cluster_pool_ids(cluster_id)
            for pool_id in pool_ids:
                pool = etcd_read_key(
                    '/clusters/%s/Pools/%s' % (
                        cluster_id,
                        pool_id
                    )
                )
                pool['integration_id'] = cluster_id
                pools[pool_id] = pool
        except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
            pass
        return pools

    def get_most_used_pools(self, cluster_name, pools):
        most_used_pools = []
        p_sort = sorted(pools.keys(), key=lambda x: (pools[x]['percent_used']))
        p_sort.reverse()
        for pool_id in p_sort:
            pool_det = pools.get(pool_id)
            pool_det['cluster_name'] = cluster_name
            pool_det['state'] = threshold_utils.get_utilization_state(
                utilization_type=CephUtilizationTypes.POOL_UTILIZATION,
                utilization_val=pool_det['percent_used'],
                integration_id=pool_det['integration_id'],
                sds_name=self.name
            )
            most_used_pools.append(pool_det)
        return most_used_pools[:5]

    def get_rbd_status_wise_counts(self, cluster_id, rbds):
        # No status for rbds so currently only alert counters will be available
        rbd_status_wise_counts = {
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0,
            pm_consts.TOTAL: 0
        }
        rbd_status_wise_counts[pm_consts.TOTAL] = len(rbds)
        crit_alerts, warn_alerts = parse_resource_alerts(
            'rbd',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        rbd_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ] = len(crit_alerts)
        rbd_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = len(warn_alerts)
        return rbd_status_wise_counts

    def get_pool_status_wise_counts(self, cluster_id, pools):
        # No status for pools, so only alert counters will be available
        pool_status_wise_counts = {
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0,
            pm_consts.TOTAL: 0
        }
        pool_status_wise_counts[pm_consts.TOTAL] = \
            len(pools.keys())
        crit_alerts, warn_alerts = parse_resource_alerts(
            'pool',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        pool_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ] = len(crit_alerts)
        pool_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = len(warn_alerts)
        return pool_status_wise_counts

    def get_rbd_names(self, cluster_id, pool_ids):
        rbd_names = {}
        for pool_id in pool_ids:
            try:
                etcd_pool_rbds = central_store_util.read_key(
                    '/clusters/%s/Pools/%s/Rbds' % (
                        cluster_id,
                        pool_id
                    )
                )
                for etcd_pool_rbd in etcd_pool_rbds.leaves:
                    etcd_pool_rbd_key_contents = etcd_pool_rbd.key.split('/')
                    # /clusters/a88ada59-f52b-4608-9311-96cccfbbbf6a/Pools/0/Rbds/MyBlockDevice
                    if len(etcd_pool_rbd_key_contents) == 7:
                        pool_rbds = rbd_names.get(pool_id, [])
                        pool_rbds.append(etcd_pool_rbd_key_contents[6])
                        rbd_names[pool_id] = pool_rbds
            except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
                continue
        return rbd_names

    def get_rbds(self, cluster_id, pools):
        rbds = []
        pool_ids = []
        try:
            for pool_id, pool in pools.iteritems():
                pool_ids.append(pool_id)
            rbd_names = self.get_rbd_names(cluster_id, pool_ids)
        except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
            pass
        for pool_id, pool_rbds in rbd_names.iteritems():
            for rbd in pool_rbds:
                try:
                    rbd_dict = etcd_read_key(
                        '/clusters/%s/Pools/%s/Rbds/%s' % (
                            cluster_id,
                            pool_id,
                            rbd
                        )
                    )
                    rbd_dict['integration_id'] = cluster_id
                    rbds.append(rbd_dict)
                except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
                    continue
        return rbds

    def get_most_used_rbds(self, cluster_name, rbds):
        # Needs to be tested
        most_used_rbds = []
        for index, rbd in enumerate(rbds):
            rbds[index]['cluster_name'] = cluster_name
            rbds[index]['percent_used'] = 0
            if rbd['provisioned'] > 0:
                rbds[index]['percent_used'] = (
                    int(rbd['used']) * 100 * 1.0
                ) / (
                    int(rbd['provisioned']) * 1.0
                )
            rbds[index]['state'] = threshold_utils.get_utilization_state(
                utilization_type=CephUtilizationTypes.RBD_UTILIZATION,
                utilization_val=rbds[index]['percent_used'],
                integration_id=rbds[index]['integration_id'],
                sds_name=self.name
            )
        most_used_rbds = sorted(rbds, key=lambda k: k['percent_used'])
        most_used_rbds.reverse()
        return most_used_rbds[:5]

    def get_cluster_osds(self, cluster_id):
        osds = []
        try:
            osd_data = etcd_read_key(
                '/clusters/%s/maps/osd_map' % cluster_id
            )
            osd_data = json.loads(osd_data['data'])
            osds = osd_data.get('osds')
            if not osds:
                osds = []
        except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
            pass
        return osds

    def get_osd_status_wise_counts(self, cluster_id, osds):
        osd_status_wise_counts = {
            'total': 0,
            'down': 0,
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0,
            'near_full': 0
        }
        for osd in osds:
            if 'up' not in osd.get('state', ''):
                osd_status_wise_counts['down'] = \
                    osd_status_wise_counts['down'] + 1
            osd_status_wise_counts['total'] = \
                osd_status_wise_counts['total'] + 1
        crit_alerts, warn_alerts = parse_resource_alerts(
            'osd',
            pm_consts.CLUSTER,
            cluster_id=cluster_id
        )
        for osd_alert in crit_alerts:
            if (
                osd_alert['severity'] == pm_consts.CRITICAL and
                osd_alert['resource'] == 'osd_utilization'
            ):
                osd_status_wise_counts['near_full'] = \
                    osd_status_wise_counts.get('near_full', 0) + 1
        osd_status_wise_counts[
            pm_consts.CRITICAL_ALERTS
        ] = len(crit_alerts)
        osd_status_wise_counts[
            pm_consts.WARNING_ALERTS
        ] = len(warn_alerts)
        return osd_status_wise_counts

    def get_mon_status_wise_counts(self, cluster_id):
        mon_status_wise_counts = {
            'outside_quorum': 0,
            'total': 0
        }
        try:
            mons = etcd_read_key(
                '/clusters/%s/maps/mon_map/data' % cluster_id
            )
            mons = json.loads(mons.get('data', '{}'))
            mons = mons['mons']
            mon_status_wise_counts['total'] = len(mons)
            outside_quorum = etcd_read_key(
                '/clusters/%s/maps/mon_status/data' % cluster_id
            )
            outside_quorum = json.loads(outside_quorum.get('data', '{}'))
            outside_quorum = outside_quorum.get('outside_quorum', [])
            mon_status_wise_counts['outside_quorum'] = len(outside_quorum)
        except EtcdKeyNotFound:
            pass
        except (ValueError, TendrlPerformanceMonitoringException) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Exception caught computing mon status "
                        "wise counts",
                        "exception": ex
                    }
                )
            )
        return mon_status_wise_counts

    def get_pg_counts(self, cluster_id):
        try:
            pg_summary = etcd_read_key(
                '/clusters/%s/maps/pg_summary/data' % cluster_id
            )
            pg_summary = json.loads(pg_summary.get('data', '{}'))
            if 'all' not in pg_summary:
                return {}
            pg_summary = pg_summary['all']
            if isinstance(pg_summary, basestring):
                pg_summary = ast.literal_eval(pg_summary)
            return _calculate_pg_counters(
                pg_summary
            )
        except (EtcdKeyNotFound, TendrlPerformanceMonitoringException):
            return {}

    def get_cluster_summary(self, cluster_id, cluster_name):
        ret_val = {}
        pools = self.get_cluster_pools(cluster_id)
        cluster_node_ids = central_store_util.get_cluster_node_ids(cluster_id)
        rbds = self.get_rbds(cluster_id, pools)
        osds = self.get_cluster_osds(cluster_id)
        ret_val['most_used_pools'] = self.get_most_used_pools(
            cluster_name,
            pools
        )
        ret_val['services_count'] = self.get_services_count(
            cluster_node_ids
        )
        ret_val['most_used_rbds'] = self.get_most_used_rbds(
            cluster_name,
            rbds
        )
        ret_val['osd_status_wise_counts'] = self.get_osd_status_wise_counts(
            cluster_id,
            osds
        )
        ret_val['rbd_status_wise_counts'] = \
            self.get_rbd_status_wise_counts(cluster_id, rbds)
        ret_val['mon_status_wise_counts'] = \
            self.get_mon_status_wise_counts(cluster_id)
        ret_val['pool_status_wise_counts'] = \
            self.get_pool_status_wise_counts(cluster_id, pools)
        ret_val['pg_status_wise_counts'] = self.get_pg_counts(cluster_id)
        throughput = {}
        throughput['cluster_network'] = self.get_cluster_throughput(
            'cluster_network',
            central_store_util.get_cluster_node_contexts(cluster_id),
            cluster_id
        )
        throughput['public_network'] = self.get_cluster_throughput(
            'public_network',
            central_store_util.get_cluster_node_contexts(cluster_id),
            cluster_id
        )
        ret_val['throughput'] = throughput
        return ret_val

    def get_system_throughput(self, cluster_summaries):
        cluster_throughput = 0.0
        public_throughput = 0.0
        cnt = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_throughput = \
                    cluster_throughput + cluster_summary.sds_det.get(
                        'throughput',
                        {}
                    ).get('cluster_network')
                cnt = cnt + 1
        if cnt > 0:
            cluster_throughput = (cluster_throughput * 1.0) / (cnt * 1.0)
        NS.time_series_db_manager.get_plugin().push_metrics(
            NS.time_series_db_manager.get_timeseriesnamefromresource(
                sds_type=self.name,
                network_type='cluster_network',
                resource_name=pm_consts.SYSTEM_THROUGHPUT,
                utilization_type=pm_consts.USED
            ),
            cluster_throughput
        )
        cnt = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                public_throughput = \
                    public_throughput + cluster_summary.sds_det.get(
                        'throughput',
                        {}
                    ).get('public_network')
                cnt = cnt + 1
        if cnt > 0:
            public_throughput = (public_throughput * 1.0) / (cnt * 1.0)
        NS.time_series_db_manager.get_plugin().push_metrics(
            NS.time_series_db_manager.get_timeseriesnamefromresource(
                sds_type=self.name,
                network_type='public_network',
                resource_name=pm_consts.SYSTEM_THROUGHPUT,
                utilization_type=pm_consts.USED
            ),
            public_throughput
        )
        return {
            'cluster_network': cluster_throughput,
            'public_network': public_throughput
        }

    def get_system_pg_counts(self, cluster_summaries):
        system_pg_counts = {}
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_pg_counts = cluster_summary.sds_det.get(
                    'pg_status_wise_counts',
                    {}
                )
                for status, counter in cluster_pg_counts.iteritems():
                    counts = system_pg_counts.get('status', 0)
                    system_pg_counts[status] = counts + counter
        return system_pg_counts

    def get_system_mon_status_wise_counts(self, cluster_summaries):
        mon_status_wise_counts = {}
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_mon_count = \
                    cluster_summary.sds_det.get('mon_status_wise_counts', {})
                if (
                    isinstance(cluster_mon_count, unicode) and not
                        isinstance(cluster_mon_count, dict)
                ):
                    cluster_mon_count = ast.literal_eval(
                        cluster_mon_count.encode('ascii', 'ignore')
                    )
                for status, count in cluster_mon_count.iteritems():
                    mon_status_wise_counts[status] = \
                        mon_status_wise_counts.get(status, 0) + int(count)
        return mon_status_wise_counts

    def get_system_osd_status_wise_counts(self, cluster_summaries):
        osd_status_wise_counts = {}
        osd_critical_alerts = 0
        osd_warning_alerts = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_osd_count = \
                    cluster_summary.sds_det.get('osd_status_wise_counts', {})
                if (
                    isinstance(cluster_osd_count, unicode) and not
                        isinstance(cluster_osd_count, dict)
                ):
                    cluster_osd_count = ast.literal_eval(
                        cluster_osd_count.encode('ascii', 'ignore')
                    )
                for status, count in cluster_osd_count.iteritems():
                    if isinstance(count, int):
                        osd_status_wise_counts[status] = \
                            osd_status_wise_counts.get(status, 0) + count
                osd_critical_alerts = \
                    osd_critical_alerts + cluster_osd_count.get(
                        pm_consts.CRITICAL_ALERTS
                    )
                osd_warning_alerts = \
                    osd_warning_alerts + cluster_osd_count.get(
                        pm_consts.WARNING_ALERTS
                    )
        osd_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            osd_warning_alerts
        osd_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            osd_critical_alerts
        return osd_status_wise_counts

    def get_system_max_used_pools(self, cluster_summaries):
        pools = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_most_used_pools = \
                    cluster_summary.sds_det.get('most_used_pools', {})
                if isinstance(cluster_most_used_pools, basestring):
                    cluster_most_used_pools = ast.literal_eval(
                        cluster_most_used_pools.encode('ascii', 'ignore')
                    )
                for pool in cluster_most_used_pools:
                    if isinstance(pool, unicode):
                        pool = pool.encode('ascii', 'ignore')
                        pool = ast.literal_eval(pool)
                    pools.append(pool)
        most_used_pools = \
            sorted(pools, key=lambda k: k['percent_used'])
        most_used_pools.reverse()
        return most_used_pools[:5]

    def get_system_max_used_rbds(self, cluster_summaries):
        rbds = []
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_most_used_rbds = \
                    cluster_summary.sds_det.get('most_used_rbds', {})
                if (
                    isinstance(cluster_most_used_rbds, unicode) and not
                        isinstance(cluster_most_used_rbds, list)
                ):
                    cluster_most_used_rbds = ast.literal_eval(
                        cluster_most_used_rbds.encode('ascii', 'ignore')
                    )
                rbds.extend(cluster_most_used_rbds)
        most_used_rbds = \
            sorted(rbds, key=lambda k: k['percent_used'])
        most_used_rbds.reverse()
        return most_used_rbds[:5]

    def get_system_pool_status_wise_counts(self, cluster_summaries):
        pool_status_wise_counts = {}
        pool_critical_alerts = 0
        pool_warning_alerts = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_pool_count = \
                    cluster_summary.sds_det.get('pool_status_wise_counts', {})
                if (
                    isinstance(cluster_pool_count, unicode) and not
                        isinstance(cluster_pool_count, dict)
                ):
                    cluster_pool_count = ast.literal_eval(
                        cluster_pool_count.encode('ascii', 'ignore')
                    )
                for status, count in cluster_pool_count.iteritems():
                    if isinstance(count, int):
                        pool_status_wise_counts[status] = \
                            pool_status_wise_counts.get(status, 0) + count
                pool_critical_alerts = \
                    pool_critical_alerts + cluster_pool_count.get(
                        pm_consts.CRITICAL_ALERTS
                    )
                pool_warning_alerts = \
                    pool_warning_alerts + cluster_pool_count.get(
                        pm_consts.WARNING_ALERTS
                    )
        pool_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            pool_warning_alerts
        pool_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            pool_critical_alerts
        return pool_status_wise_counts

    def get_system_rbd_status_wise_counts(self, cluster_summaries):
        rbd_status_wise_counts = {}
        rbd_critical_alerts = 0
        rbd_warning_alerts = 0
        for cluster_summary in cluster_summaries:
            if self.name in cluster_summary.sds_type:
                cluster_rbd_count = \
                    cluster_summary.sds_det.get('rbd_status_wise_counts', {})
                if (
                    isinstance(cluster_rbd_count, unicode) and not
                        isinstance(cluster_rbd_count, dict)
                ):
                    cluster_rbd_count = ast.literal_eval(
                        cluster_rbd_count.encode('ascii', 'ignore')
                    )
                for status, count in cluster_rbd_count.iteritems():
                    if isinstance(count, int):
                        rbd_status_wise_counts[status] = \
                            rbd_status_wise_counts.get(status, 0) + count
                rbd_critical_alerts = \
                    rbd_critical_alerts + cluster_rbd_count.get(
                        pm_consts.CRITICAL_ALERTS
                    )
                rbd_warning_alerts = \
                    rbd_warning_alerts + cluster_rbd_count.get(
                        pm_consts.WARNING_ALERTS
                    )
        rbd_status_wise_counts[pm_consts.WARNING_ALERTS] = \
            rbd_warning_alerts
        rbd_status_wise_counts[pm_consts.CRITICAL_ALERTS] = \
            rbd_critical_alerts
        return rbd_status_wise_counts

    def compute_system_summary(self, cluster_summaries):
        try:
            SystemSummary(
                utilization=self.get_system_utilization(cluster_summaries),
                hosts_count=self.get_system_host_status_wise_counts(
                    cluster_summaries
                ),
                cluster_count=self.get_clusters_status_wise_counts(
                    cluster_summaries
                ),
                sds_det={
                    'mon_status_wise_counts': self.get_system_mon_status_wise_counts(
                        cluster_summaries
                    ),
                    'osd_status_wise_counts': self.get_system_osd_status_wise_counts(
                        cluster_summaries
                    ),
                    'pool_status_wise_counts': self.get_system_pool_status_wise_counts(
                        cluster_summaries
                    ),
                    'rbd_status_wise_counts': self.get_system_rbd_status_wise_counts(
                        cluster_summaries
                    ),
                    'most_used_pools': self.get_system_max_used_pools(
                        cluster_summaries
                    ),
                    'most_used_rbds': self.get_system_max_used_rbds(
                        cluster_summaries
                    ),
                    'pg_status_wise_counts': self.get_system_pg_counts(
                        cluster_summaries
                    ),
                    'throughput': self.get_system_throughput(cluster_summaries)
                },
                sds_type=self.name
            ).save(update=False)
        except (
            AttributeError,
            ValueError,
            TypeError,
            KeyError
        ) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": "Exception caught computing system "
                                        "summary.",
                             "exception": ex
                             }
                )
            )

    def get_node_osd_status_wise_counts(self, node_id):
        osds_in_node = []
        osd_status_wise_counts = {
            'total': 0,
            'down': 0,
            pm_consts.CRITICAL_ALERTS: 0,
            pm_consts.WARNING_ALERTS: 0
        }
        cluster_id = central_store_util.get_node_cluster_id(
            node_id
        )
        node_ip = ''
        ip_indexes = etcd_read_key('/indexes/ip')
        for ip, indexed_node_id in ip_indexes.iteritems():
            if node_id == indexed_node_id:
                node_ip = ip
        try:
            osds = etcd_read_key(
                '/clusters/%s/maps/osd_map/data/osds' % cluster_id
            )
            osds = ast.literal_eval(osds.get('osds', '[]'))
            for osd in osds:
                if (
                    node_ip in osd.get('cluster_addr', '') or
                    node_ip in osd.get('public_addr', '')
                ):
                    osds_in_node.append(osd.get('osd'))
                    if 'up' not in osd.get('state'):
                        osd_status_wise_counts['down'] = \
                            osd_status_wise_counts['down'] + 1
                    osd_status_wise_counts['total'] = \
                        osd_status_wise_counts['total'] + 1
            crit_alerts, warn_alerts = parse_resource_alerts(
                'osd',
                pm_consts.CLUSTER,
                cluster_id=cluster_id
            )
            count = 0
            for alert in crit_alerts:
                plugin_instance = alert['tags'].get('plugin_instance', '')
                if int(plugin_instance[len('osd_'):]) in osds_in_node:
                    count = count + 1
            osd_status_wise_counts[
                pm_consts.CRITICAL_ALERTS
            ] = count
            count = 0
            for alert in warn_alerts:
                plugin_instance = alert['tags'].get('plugin_instance', '')
                if int(plugin_instance[len('osd_'):]) in osds_in_node:
                    count = count + 1
            osd_status_wise_counts[
                pm_consts.WARNING_ALERTS
            ] = count
        except (
            EtcdException,
            AttributeError,
            KeyError,
            ValueError,
            TendrlPerformanceMonitoringException
        ) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Exception caught computing node osd "
                        "counts",
                        "exception": ex
                    }
                )
            )
        return osd_status_wise_counts

    def get_node_summary(self, node_id):
        ret_val = {}
        ret_val['osd_status_wise_counts'] = self.get_node_osd_status_wise_counts(
            node_id
        )
        return ret_val
