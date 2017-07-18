from abc import abstractmethod
import importlib
import inspect
import os
import re
import six
from string import Template

from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException


class FailedToFetchTimeSeriesData(Exception):

    def __init__(self, err_msg):
        self.msg = err_msg


class PluginMount(type):

    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'plugins'):
            cls.plugins = []
        else:
            cls.register_plugin(cls)

    def register_plugin(cls, plugin):
        instance = plugin()
        cls.plugins.append(instance)
        instance.intialize()


@six.add_metaclass(PluginMount)
class TimeSeriesDBPlugin(object):

    @abstractmethod
    def intialize(self):
        raise NotImplementedError()

    @abstractmethod
    def get_metric_stats(
        self,
        entity_name,
        metric_name,
        time_interval=None,
        start_time=None,
        end_time=None
    ):
        raise NotImplementedError()

    @abstractmethod
    def get_metrics(self, entity_name):
        raise NotImplementedError()

    @abstractmethod
    def push_metrics(self, metric_name, metric_value):
        raise NotImplementedError()

    @abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abstractmethod
    def get_utilizationtype(self, resource_name, utilization_type):
        raise NotImplementedError()

    @abstractmethod
    def get_delimeter(self):
        raise NotImplementedError()

    @abstractmethod
    def get_aggregated_stats(
        self,
        aggregation_type,
        entity_names,
        metric_name
    ):
        raise NotImplementedError()

    @abstractmethod
    def get_node_disk_iops_stats(
        self,
        node_id,
        time_interval=None,
        start_time=None,
        end_time=None
    ):
        raise NotImplementedError()

    @abstractmethod
    def parse_time(self, time_str):
        raise NotImplementedError()


class TimeSeriesDBManager(object):

    def __init__(self):
        # Since this is a singleton class the singleton framework ensures only
        # a single call to this constructor in the life time of the application
        # However wherever the class is attempted to be intialized, it tries to
        # match the constructor and hence the 2nd arguement is made to appear
        # as an optional arguement although it is enforced internally not be
        # optional due to reason stated above.
        self.time_series_db = NS.performance_monitoring.config.data[
            'time_series_db']
        try:
            self.load_plugins()
        except (SyntaxError, ValueError, ImportError) as ex:
            raise ex
        self.plugin = None
        self.set_plugin()

    def load_plugins(self):
        try:
            path = os.path.dirname(os.path.abspath(__file__)) + '/dbplugins'
            pkg = 'tendrl.performance_monitoring.time_series_db.dbplugins'
            for py in [f[:-3] for f in os.listdir(path)
                       if f.endswith('.py') and f != '__init__.py']:
                plugin_name = '.'.join([pkg, py])
                mod = importlib.import_module(plugin_name)
                clsmembers = inspect.getmembers(mod, inspect.isclass)
                for name, cls in clsmembers:
                    exec("from %s import %s" % (plugin_name, name))
        except (SyntaxError, ValueError, ImportError) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Failed to load the time series db '
                                        'plugins.',
                             "exception": ex
                             }
                )
            )
            raise ex

    def get_plugin(self):
        return self.plugin

    def set_plugin(self):
        for plugin in TimeSeriesDBPlugin.plugins:
            if re.search(self.time_series_db.lower(), type(
                    plugin).__name__.lower(), re.IGNORECASE):
                self.plugin = plugin

    def stop(self):
        self.plugin.destroy()

    def get_timeseriesnamefromresource(self, **kwargs):
        # If in future this function starts to appear more plugin
        # specific move it from here to respecive TimeSeriesDBPlugin
        delimeter = self.get_plugin().get_delimeter()
        resource_name = kwargs['resource_name']
        if 'utilization_type' in kwargs:
            kwargs['utilization_type'] = self.get_plugin().get_utilizationtype(
                resource_name,
                kwargs['utilization_type']
            )
        pattern = {
            pm_consts.SYSTEM_UTILIZATION: '$sds_type{0}utilization{0}'
            '$utilization_type',
            pm_consts.CLUSTER_UTILIZATION: 'cluster_$integration_id{0}'
            'cluster_utilization{0}$utilization_type',
            pm_consts.CLUSTER_THROUGHPUT: 'cluster_$integration_id{0}'
            'throughput{0}$network_type{0}$utilization_type',
            pm_consts.SYSTEM_THROUGHPUT: '$sds_type{0}'
            'throughput{0}$network_type{0}$utilization_type',
            pm_consts.NODE_THROUGHPUT: '$node_name{0}'
            'network_throughput-$network_type{0}$utilization_type',
            pm_consts.LATENCY: 'ping{0}ping-$underscored_monitoring_node_name',
            pm_consts.IOPS: 'cluster_$integration_id{0}cluster_iops_read_write{0}'
            '$utilization_type',
            pm_consts.SWAP: 'swap{0}$utilization_type',
            pm_consts.SWAP_TOTAL: '$utilization_type',
            pm_consts.CPU: '$underscored_node_name{0}cpu{0}cpu_system_user{0}'
            '$utilization_type',
            pm_consts.STORAGE: '$underscored_node_name{0}storage{0}'
            '$utilization_type',
            pm_consts.CLUSTER_IOPS: 'cluster_$integration_id{0}'
            'cluster_iops_read_write{0}gauge-total'
        }
        if not pattern.get(resource_name):
            raise TendrlPerformanceMonitoringException(
                'No pattern found for the requested resource %s.'
            )
        return Template(
            pattern.get(resource_name).format(delimeter)
        ).substitute(kwargs)
