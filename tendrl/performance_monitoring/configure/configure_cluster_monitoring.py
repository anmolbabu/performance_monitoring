import gevent
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util
from tendrl.performance_monitoring.utils.util import initiate_config_generation


class ConfigureClusterMonitoring(gevent.greenlet.Greenlet):
    def __init__(self):
        super(ConfigureClusterMonitoring, self).__init__()
        self._complete = gevent.event.Event()

    def configure_cluster_monitoring(self):
        integration_ids = []
        while not self._complete.is_set():
            try:
                integration_ids = central_store_util.get_integration_ids()
            except (AttributeError, EtcdException)as ex:
                Event(
                    ExceptionMessage(
                        priority="debug",
                        publisher=NS.publisher_id,
                        payload={
                            "message": 'Error fetcing list of cluster ids to '
                            'configure',
                            "exception": ex
                        }
                    )
                )
                pass
            for integration_id in integration_ids:
                configs = NS.sds_monitoring_manager.configure_monitoring(
                    integration_id
                )
                if configs:
                    for config in configs:
                        gevent.sleep(0.1)
                        gevent.spawn(initiate_config_generation, config)
            gevent.sleep(10)

    def _run(self):
        self.configure_cluster_monitoring()

    def stop(self):
        self._complete.set()
