from etcd import EtcdKeyNotFound
import gevent
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.objects.cluster_summary \
    import ClusterSummary
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
from tendrl.performance_monitoring.utils.util import get_latest_stat


class ClusterSummarise(gevent.greenlet.Greenlet):
    def __init__(self):
        super(ClusterSummarise, self).__init__()
        self._complete = gevent.event.Event()

    def parse_host_count(self, cluster_id):
        status_wise_count = {
            'total': 0,
            'down': 0,
            'crit_alert_count': 0,
            'warn_alert_count': 0
        }
        cluster_nodes = central_store_util.get_cluster_node_ids(cluster_id)
        for node_id in cluster_nodes:
            try:
                node_context = central_store_util.read(
                    '/clusters/%s/nodes/%s/NodeContext' % (
                        cluster_id,
                        node_id
                    )
                )
            except EtcdKeyNotFound as ex:
                Event(
                    ExceptionMessage(
                        priority="debug",
                        publisher=NS.publisher_id,
                        payload={
                            "message": 'Failed to fetch node-context from'
                            ' /clusters/%s/nodes/%s/NodeContext' % (
                                cluster_id,
                                node_id
                            ),
                            "exception": ex
                        }
                    )
                )
            status = node_context.get('status')
            if status:
                if status != 'UP':
                    status_wise_count['down'] = status_wise_count['down'] + 1
            status_wise_count['total'] = status_wise_count['total'] + 1
            alerts = []
            try:
                alerts = central_store_util.get_node_alerts(node_id)
            except EtcdKeyNotFound:
                pass
            for alert in alerts:
                if alert.get('severity') == 'CRITICAL':
                    status_wise_count['crit_alert_count'] = \
                        status_wise_count['crit_alert_count'] + 1
                elif alert.get('severity') == 'WARNING':
                    status_wise_count['warn_alert_count'] = \
                        status_wise_count['warn_alert_count'] + 1
        return status_wise_count

    def cluster_nodes_summary(self, cluster_id):
        node_summaries = []
        node_ids = central_store_util.get_cluster_node_ids(cluster_id)
        for node_id in node_ids:
            try:
                node_summary = central_store_util.read(
                    '/monitoring/summary/nodes/%s' % node_id
                )
                node_summaries.append(node_summary)
            except EtcdKeyNotFound as ex:
                Event(
                    ExceptionMessage(
                        priority="debug",
                        publisher=NS.publisher_id,
                        payload={
                            "message": 'Error caught fetching node summary of'
                            ' node %s.' % node_id,
                            "exception": ex
                        }
                    )
                )
                continue
        return node_summaries

    def get_cluster_iops(self, cluster_id):
        try:
            entity_name, metric_name = NS.time_series_db_manager.\
                get_timeseriesnamefromresource(
                    resource_name=pm_consts.CLUSTER_IOPS,
                    cluster_id=cluster_id
                ).split(
                    NS.time_series_db_manager.get_plugin().get_delimeter(),
                    1
                )
            cluster_iops = get_latest_stat(entity_name, metric_name)
            return cluster_iops
        except TendrlPerformanceMonitoringException:
            return pm_consts.NOT_AVAILABLE

    def parse_cluster(self, cluster_id):
        utilization = central_store_util.read(
            '/clusters/%s/Utilization' % cluster_id
        )
        used = 0
        total = 0
        percent_used = 0
        if utilization.get('used_capacity'):
            used = utilization.get('used_capacity')
        elif utilization.get('used'):
            used = utilization.get('used')
        if utilization.get('raw_capacity'):
            total = utilization.get('raw_capacity')
        elif utilization.get('total'):
            total = utilization.get('total')
        if utilization.get('pcnt_used'):
            percent_used = utilization.get('pcnt_used')
        return ClusterSummary(
            utilization={
                'total': int(total),
                'used': int(used),
                'percent_used': float(percent_used)
            },
            iops=str(self.get_cluster_iops(cluster_id)),
            hosts_count=self.parse_host_count(cluster_id),
            sds_type=central_store_util.get_cluster_sds_name(cluster_id),
            node_summaries=self.cluster_nodes_summary(
                cluster_id
            ),
            sds_det=NS.sds_monitoring_manager.get_cluster_summary(
                cluster_id,
                central_store_util.get_cluster_name(cluster_id)
            ),
            cluster_id=cluster_id,
        )

    def _run(self):
        while not self._complete.is_set():
            cluster_summaries = []
            clusters = central_store_util.get_cluster_ids()
            for clusterid in clusters:
                gevent.sleep(0.1)
                try:
                    cluster_summary = self.parse_cluster(clusterid)
                    cluster_summaries.append(cluster_summary.copy())
                    cluster_summary.save(update=False)
                except EtcdKeyNotFound:
                    pass
                except Exception as ex:
                    Event(
                        ExceptionMessage(
                            priority="debug",
                            publisher=NS.publisher_id,
                            payload={
                                "message": 'Error caught computing summary.',
                                "exception": ex
                            }
                        )
                    )
                    continue
            NS.sds_monitoring_manager.compute_system_summary(
                cluster_summaries
            )
            gevent.sleep(60)

    def stop(self):
        self._complete.set()
