import datetime
from etcd import EtcdKeyNotFound
from etcd import EtcdException
import gevent
import math
from pytz import utc
import urllib3

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.utils.time_utils import now as tendrl_now

from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.objects.node_summary \
    import NodeSummary
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
import tendrl.performance_monitoring.utils.threshold_utils as threshold_utils
from tendrl.performance_monitoring.utils.util import get_latest_node_stat
from tendrl.performance_monitoring.utils.util import get_latest_stats


class NodeSummarise(gevent.greenlet.Greenlet):
    def __init__(self):
        super(NodeSummarise, self).__init__()
        self._complete = gevent.event.Event()

    def get_net_host_cpu_utilization(self, node):
        try:
            percent_user = get_latest_node_stat(node, 'cpu.percent-user')
            percent_system = get_latest_node_stat(node, 'cpu.percent-system')
            node_name = central_store_util.get_node_name_from_id(
                node
            )
            NS.time_series_db_manager.get_plugin().push_metrics(
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    underscored_node_name=node_name.replace('.', '_'),
                    resource_name=pm_consts.CPU,
                    utilization_type=pm_consts.PERCENT_USED
                ),
                percent_user + percent_system
            )
            return {
                'percent_used': str(percent_user + percent_system),
                'state': threshold_utils.get_utilization_state(
                    utilization_type=threshold_utils.UtilizationTypes.CPU,
                    utilization_val=(percent_user + percent_system),
                    node_id=node
                ),
                'updated_at': datetime.datetime.now().isoformat()
            }
        except (
            ValueError,
            urllib3.exceptions.HTTPError,
            TendrlPerformanceMonitoringException
        ):
            # Exception already handled
            return None

    def get_net_host_memory_utilization(self, node):
        try:
            used = get_latest_node_stat(node, 'memory.memory-used')
            total = get_latest_node_stat(node, 'aggregation-memory-sum.memory')
            percent_used = get_latest_node_stat(node, 'memory.percent-used')
            return {
                'used': str(used),
                'percent_used': str(percent_used),
                'total': str(total),
                'state': threshold_utils.get_utilization_state(
                    utilization_type=threshold_utils.UtilizationTypes.MEMORY,
                    utilization_val=percent_used,
                    node_id=node
                ),
                'updated_at': datetime.datetime.now().isoformat()
            }
        except (
            urllib3.exceptions.HTTPError,
            ValueError,
            TendrlPerformanceMonitoringException
        ):
            # Exception already handled
            return None

    def get_net_host_swap_utilization(self, node):
        try:
            metric_name = \
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    resource_name=pm_consts.SWAP,
                    utilization_type=pm_consts.USED
                )
            used = get_latest_node_stat(node, metric_name)
            metric_name = \
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    resource_name=pm_consts.SWAP_TOTAL,
                    utilization_type=pm_consts.TOTAL
                )
            total = get_latest_node_stat(node, metric_name)
            metric_name = \
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    resource_name=pm_consts.SWAP,
                    utilization_type=pm_consts.PERCENT_USED
                )
            percent_used = get_latest_node_stat(node, metric_name)
            return {
                'used': str(used),
                'percent_used': str(percent_used),
                'total': str(total),
                'state': threshold_utils.get_utilization_state(
                    utilization_type=threshold_utils.UtilizationTypes.SWAP,
                    utilization_val=percent_used,
                    node_id=node
                ),
                'updated_at': datetime.datetime.now().isoformat()
            }
        except (
            ValueError,
            urllib3.exceptions.HTTPError,
            TendrlPerformanceMonitoringException
        ):
            # No need to log this exception as it is logged in generic function
            # but need to return None to indeicate failure to fetch instead of
            # dummy 0
            return None

    def get_net_storage_utilization(self, node):
        try:
            used_stats = get_latest_stats(node, 'df-*.df_complex-used')
            used = 0.0
            for stat in used_stats:
                if not math.isnan(float(stat)):
                    used = used + float(stat)
            free_stats = get_latest_stats(node, 'df-*.df_complex-free')
            free = 0.0
            for stat in free_stats:
                if not math.isnan(float(stat)):
                    free = free + float(stat)
            if free + used == 0:
                return None
            percent_used = float(used * 100) / float(free + used)
            node_name = central_store_util.get_node_name_from_id(
                node
            )
            NS.time_series_db_manager.get_plugin().push_metrics(
                NS.time_series_db_manager.get_timeseriesnamefromresource(
                    underscored_node_name=node_name.replace('.', '_'),
                    resource_name=pm_consts.STORAGE,
                    utilization_type=pm_consts.PERCENT_USED
                ),
                percent_used
            )
            return {
                'used': str(used),
                'total': str(used + free),
                'percent_used': str(percent_used),
                'state': threshold_utils.get_utilization_state(
                    utilization_type=threshold_utils.UtilizationTypes.MOUNT_POINT,
                    utilization_val=percent_used,
                    node_id=node
                ),
                'updated_at': datetime.datetime.now().isoformat()
            }
        except (
            ValueError,
            urllib3.exceptions.HTTPError,
            TendrlPerformanceMonitoringException
        ):
            # Exception already handled
            return None

    def get_alert_count(self, node):
        try:
            alert_ids = central_store_util.get_node_alert_ids(node)
            return len(alert_ids)
        except (AttributeError, EtcdException):
            return 0

    def calculate_host_summary(self, node):
        gevent.sleep(0.1)
        cpu_usage = self.get_net_host_cpu_utilization(node)
        memory_usage = self.get_net_host_memory_utilization(node)
        storage_usage = self.get_net_storage_utilization(node)
        swap_usage = self.get_net_host_swap_utilization(node)
        alert_count = self.get_alert_count(node)
        sds_det = NS.sds_monitoring_manager.get_node_summary(
            node
        )
        old_summary = NodeSummary(
            node_id=node,
            name='',
            status='',
            role='',
            cluster_name='',
            cpu_usage={
                'percent_used': '',
                'updated_at': ''
            },
            memory_usage={
                'percent_used': '',
                'updated_at': '',
                'used': '',
                'total': ''
            },
            storage_usage={
                'percent_used': '',
                'total': '',
                'used': '',
                'updated_at': ''
            },
            swap_usage={
                'percent_used': '',
                'updated_at': '',
                'used': '',
                'total': ''
            },
            sds_det={},
            alert_count=alert_count
        )
        try:
            old_summary = old_summary.load()
        except EtcdKeyNotFound:
            pass
        except (
            EtcdException,
            AttributeError
        ) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Failed to fetch previously computed '
                                        'summary from etcd.',
                             "exception": ex
                             }
                )
            )
            return
        if cpu_usage is None:
            cpu_usage = old_summary.cpu_usage
        if memory_usage is None:
            memory_usage = old_summary.memory_usage
        if storage_usage is None:
            storage_usage = old_summary.storage_usage
        if swap_usage is None:
            swap_usage = old_summary.swap_usage
        selinux_mode = ''
        try:
            selinux_mode = central_store_util.get_node_selinux_mode(
                node
            )
        except (
            EtcdKeyNotFound,
            AttributeError,
            EtcdException
        ):
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": 'Selinux mode not available for node '
                        '%s' % str(node),
                        "exception": ex
                    }
                )
            )
        node_name = ''
        try:
            node_name = central_store_util.get_node_name_from_id(node)
        except (
            AttributeError,
            EtcdKeyNotFound,
            EtcdException
        ) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={
                        "message": 'Node name of %s fetch failed '
                        '%s' % str(node),
                        "exception": ex
                    }
                )
            )
        try:
            summary = NodeSummary(
                name=node_name,
                node_id=node,
                status=self.get_node_status(node),
                role=central_store_util.get_node_role(node),
                cluster_name=central_store_util.get_node_cluster_name(
                    node
                ),
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                storage_usage=storage_usage,
                swap_usage=swap_usage,
                selinux_mode=selinux_mode,
                sds_det=sds_det,
                alert_count=alert_count
            )
            summary.save(update=False)
        except (AttributeError, EtcdException) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Exception caught while trying to '
                                        'save summary for node %s' % str(node),
                             "exception": ex
                             }
                )
            )

    def get_node_status(self, node_id):
        last_seen_at = central_store_util.get_node_last_seen_at(node_id)
        if last_seen_at:
            interval = (
                tendrl_now() -
                datetime.datetime.strptime(
                    last_seen_at[:-6],
                    "%Y-%m-%dT%H:%M:%S.%f"
                ).replace(tzinfo=utc)
            ).total_seconds()
            if interval < 5:
                return pm_consts.STATUS_UP
            else:
                return pm_consts.STATUS_DOWN
        return pm_consts.STATUS_NOT_MONITORED

    def calculate_host_summaries(self):
        nodes = central_store_util.get_node_ids()
        for node in nodes:
            self.calculate_host_summary(node)

    def _run(self):
        while not self._complete.is_set():
            self.calculate_host_summaries()
            gevent.sleep(60)

    def stop(self):
        self._complete.set()
