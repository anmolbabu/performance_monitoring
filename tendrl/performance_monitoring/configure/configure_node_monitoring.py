import ast
from etcd import EtcdException
import gevent.event
import gevent.greenlet
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring.utils.util import initiate_config_generation
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
import tendrl.performance_monitoring.utils.threshold_utils as threshold_utils


class ConfigureNodeMonitoring(gevent.greenlet.Greenlet):
    def init_monitoring(self):
        try:
            node_dets = central_store_util.get_nodes_details()
            for node_det in node_dets:
                self.init_monitoring_on_node(node_det)
        except (
            AttributeError,
            KeyError,
            ValueError,
            EtcdException
        ) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Failed to intialize monitoring '
                                        'configuration on nodes. ',
                             "exception": ex
                             }
                )
            )
            raise ex

    def init_monitoring_on_node(self, node_det):
        # TODO(Anmol) Ideally per cluster(node's cluster) fetch config but as
        # it is defautls for now fetching only once
        gevent.sleep(0.1)
        initiate_config_generation(
            {
                'node_id': node_det['node_id'],
                'fqdn': node_det['fqdn'],
                'plugin': 'collectd',
                'plugin_conf': {
                    'master_name': NS.performance_monitoring.config.data[
                        'master_name'],
                    'interval': NS.performance_monitoring.config.data[
                        'interval']
                }
            }
        )
        gevent.sleep(0.1)
        time_series_db_server = NS.performance_monitoring.config.data[
            'time_series_db_server'
        ]
        initiate_config_generation(
            {
                'node_id': node_det['node_id'],
                'fqdn': node_det['fqdn'],
                'plugin': 'dbpush',
                'plugin_conf': {
                    'master_name': NS.performance_monitoring.config.data[
                        'master_name'],
                    'interval': NS.performance_monitoring.config.data[
                        'interval'],
                    'time_series_db_server': time_series_db_server
                }
            }
        )
        initiate_config_generation(
            {
                'node_id': node_det['node_id'],
                'fqdn': node_det['fqdn'],
                'plugin': 'latency',
                'plugin_conf': {
                    'master_name': NS.performance_monitoring.config.data[
                        'master_name'],
                    'interval': NS.performance_monitoring.config.data[
                        'interval']
                }
            }
        )
        initiate_config_generation(
            {
                'node_id': node_det['node_id'],
                'fqdn': node_det['fqdn'],
                'plugin': 'disk',
                'plugin_conf': {
                    'master_name': NS.performance_monitoring.config.data[
                        'master_name'],
                    'interval': NS.performance_monitoring.config.data[
                        'interval']
                }
            }
        )
        config = threshold_utils.get_thresholds(
            node_id=node_det['node_id']
        )
        if isinstance(config, basestring):
            config = ast.literal_eval(config.encode('ascii', 'ignore'))
        for plugin, plugin_config in config.iteritems():
            if isinstance(plugin_config, basestring):
                plugin_config = ast.literal_eval(
                    plugin_config.encode('ascii', 'ignore')
                )
            gevent.sleep(0.1)
            initiate_config_generation(
                {
                    'node_id': node_det['node_id'],
                    'fqdn': node_det['fqdn'],
                    'plugin': plugin,
                    'plugin_conf': plugin_config
                }
            )

    def __init__(self):
        super(ConfigureNodeMonitoring, self).__init__()
        self._complete = gevent.event.Event()

    def _run(self):
        try:
            while not self._complete.is_set():
                gevent.sleep(0.1)
                self.init_monitoring()
                gevent.sleep(10)
        except (
            AttributeError,
            KeyError,
            ValueError,
            EtcdException
        ) as e:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Exception caught while watching '
                                        'alerts',
                             "exception": e
                             }
                )
            )

    def stop(self):
        self._complete.set()
