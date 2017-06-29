import etcd
from flask import Flask
from flask import request
from flask import Response
import json
import multiprocessing
import os
import signal
import socket
from uuid import UUID
import urllib3
from tendrl.commons.config import ConfigNotFound
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.message import Message
from tendrl.commons import TendrlNS
from tendrl.performance_monitoring import PerformanceMonitoringNS
from tendrl.performance_monitoring.aggregator.cluster_summary \
    import ClusterSummarise
from tendrl.performance_monitoring.aggregator.node_summary import NodeSummarise
from tendrl.performance_monitoring.configure.configure_cluster_monitoring\
    import ConfigureClusterMonitoring
from tendrl.performance_monitoring.configure.configure_node_monitoring \
    import ConfigureNodeMonitoring
from tendrl.performance_monitoring import constants as \
    pm_consts
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.sds import SDSMonitoringManager
from tendrl.performance_monitoring.time_series_db.manager \
    import TimeSeriesDBManager
import tendrl.performance_monitoring.utils.central_store_util \
    as central_store_util


app = Flask(__name__)


@app.route("/monitoring/nodes/<node_id>/<resource_name>/stats")
def get_nodestats(node_id, resource_name):
    try:
        node_name = central_store_util.get_node_name_from_id(
            node_id
        )
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        return Response(
            NS.time_series_db_manager.\
            get_plugin().\
            get_metric_stats(
                node_name,
                resource_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        TypeError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route(
    "/monitoring/clusters/<integration_id>/utilization/<utiliation_type>/stats"
)
def get_clusterutilization(integration_id, utiliation_type):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        entity_name, metric_name = NS.time_series_db_manager.\
            get_timeseriesnamefromresource(
                resource_name=pm_consts.CLUSTER_UTILIZATION,
                utilization_type=utiliation_type,
                integration_id=integration_id
            ).split(
                NS.time_series_db_manager.get_plugin().get_delimeter(),
                1
            )
        # Validate integration_id. Attempt to fetch clusters/integration_id fails
        # with EtcdKeyNotFound if cluster if is invalid
        central_store_util.read_key('/clusters/%s' % integration_id)
        return Response(
            NS.time_series_db_manager.\
            get_plugin().\
            get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        TypeError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route("/monitoring/clusters/<integration_id>/latency/stats")
def get_cluster_latency(integration_id):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        nodes_in_cluster = central_store_util.get_node_names_in_cluster(
            integration_id
        )
        metric_name = NS.time_series_db_manager.get_timeseriesnamefromresource(
            resource_name=pm_consts.LATENCY,
            underscored_monitoring_node_name=socket.getfqdn().replace('.', '_')
        )
        return Response(
            NS.time_series_db_manager.get_plugin().get_aggregated_stats(
                pm_consts.AVERAGE,
                nodes_in_cluster,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        etcd.Exception,
        AttributeError,
        urllib3.exceptions.HTTPError,
        ValueError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route(
    "/monitoring/clusters/<integration_id>/iops/stats"
)
def get_cluster_iops(integration_id):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        entity_name, metric_name = NS.time_series_db_manager.\
            get_timeseriesnamefromresource(
                integration_id=integration_id,
                resource_name=pm_consts.IOPS,
                utilization_type=pm_consts.TOTAL
            ).split(
                NS.time_series_db_manager.get_plugin().get_delimeter(),
                1
            )
        # Validate integration_id. Attempt to fetch clusters/integration_id fails
        # with EtcdKeyNotFound if cluster if is invalid
        central_store_util.read_key('/clusters/%s' % integration_id)
        return Response(
            NS.time_series_db_manager.get_plugin().get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        TypeError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route(
    "/monitoring/clusters/<integration_id>/throughput/<network_type>/stats"
)
def get_clusterthroughput(integration_id, network_type):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        entity_name, metric_name = NS.time_series_db_manager.\
            get_timeseriesnamefromresource(
                integration_id=integration_id,
                network_type=network_type,
                resource_name=pm_consts.CLUSTER_THROUGHPUT,
                utilization_type=pm_consts.USED
            ).split(
                NS.time_series_db_manager.get_plugin().get_delimeter(),
                1
            )
        # Validate integration_id. Attempt to fetch clusters/integration_id fails
        # with EtcdKeyNotFound if cluster if is invalid
        central_store_util.read_key('/clusters/%s' % integration_id)
        return Response(
            NS.time_series_db_manager.\
            get_plugin().\
            get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        TypeError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route("/monitoring/system/<sds_type>/throughput/<network_type>/stats")
def get_sdsthroughput(sds_type, network_type):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        # validate sds-type
        if sds_type not in NS.sds_monitoring_manager.supported_sds:
            raise TendrlPerformanceMonitoringException(
                'Unsupported sds %s' % sds_type
            )
        entity_name, metric_name = NS.time_series_db_manager.\
            get_timeseriesnamefromresource(
                sds_type=sds_type,
                network_type=network_type,
                resource_name=pm_consts.SYSTEM_THROUGHPUT,
                utilization_type=pm_consts.USED
            ).split(
                NS.time_series_db_manager.get_plugin().get_delimeter(),
                1
            )
        return Response(
            NS.time_series_db_manager.\
            get_plugin().\
            get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        TypeError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route("/monitoring/system/<sds_type>/utilization/<utiliation_type>/stats")
def get_sdsutilization(sds_type, utiliation_type):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        # validate sds-type
        if sds_type not in NS.sds_monitoring_manager.supported_sds:
            raise TendrlPerformanceMonitoringException(
                'Unsupported sds %s' % sds_type
            )
        entity_name, metric_name = NS.time_series_db_manager.\
            get_timeseriesnamefromresource(
                resource_name=pm_consts.SYSTEM_UTILIZATION,
                utilization_type=utiliation_type,
                sds_type=sds_type
        ).split(
            NS.time_series_db_manager.get_plugin().get_delimeter(),
            1
        )
        return Response(
            NS.time_series_db_manager.\
            get_plugin().\
            get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        AttributeError,
        ValueError,
        etcd.EtcdException,
        SyntaxError,
        urllib3.exceptions.HTTPError,
        TypeError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


@app.route("/monitoring/clusters/<integration_id>/summary")
def get_cluster_summary(integration_id):
    try:
        cluster_summary = central_store_util.get_cluster_summary(
            integration_id
        )
        return Response(
            json.dumps(cluster_summary),
            status=200,
            mimetype='application/json'
        )
    except (AttributeError, etcd.EtcdException) as ex:
        return Response(
            'Failed to fetch cluster summary for cluster %s.Error %s' % (
                integration_id,
                str(ex)
            ),
            status=500,
            mimetype='application/json'
        )


@app.route("/monitoring/clusters/iops")
def get_clusters_iops():
    try:
        cluster_list = None
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
                elif request_param[0] == "integration_ids":
                    cluster_list = (request.args.items()[0][1]).split(",")
        iops = []
        ret_code = 200
        exs = ''
        if cluster_list:
            for index, node in enumerate(cluster_list):
                uuid_string = cluster_list[index].strip()
                if UUID(
                    uuid_string,
                    version=4
                ).hex == uuid_string.replace('-', ''):
                    cluster_list[index] = cluster_list[index].strip()
                else:
                    raise TendrlPerformanceMonitoringException(
                        'Cluster id %s in the parameter is not a valid '
                        'uuid' % (
                            uuid_string
                        )
                    )
            iops, ret_code, exs = \
                central_store_util.get_cluster_iops(
                    cluster_list,
                    time_interval=time_interval,
                    start_time=start_time,
                    end_time=end_time
                )
        else:
            iops, ret_code, exs = \
                central_store_util.get_cluster_iops(
                    time_interval=time_interval,
                    start_time=start_time,
                    end_time=end_time
                )
        return Response(
            json.dumps(iops),
            status=ret_code,
            mimetype='application/json'
        )
    except (
        etcd.EtcdKeyNotFound,
        ValueError,
        SyntaxError,
        urllib3.exceptions.HTTPError,
        TendrlPerformanceMonitoringException,
        TypeError
    ) as ex:
        return Response(
            str(ex),
            status=500,
            mimetype='application/json'
        )


@app.route("/monitoring/system/<cluster_type>/summary")
def get_system_summary(cluster_type):
    try:
        if cluster_type not in NS.sds_monitoring_manager.supported_sds:
            raise TendrlPerformanceMonitoringException(
                'Unsupported sds %s' % cluster_type
            )
        summary = central_store_util.get_system_summary(cluster_type)
        return Response(
            json.dumps(summary),
            status=200,
            mimetype='application/json'
        )
    except TendrlPerformanceMonitoringException as ex:
        return Response(
            'Failed to fetch %s system summary.Error %s' % (
                cluster_type,
                str(ex)
            ),
            status=500,
            mimetype='application/json'
        )


@app.route("/monitoring/nodes/<node_id>/monitored_types")
def get_stat_types(node_id):
    try:
        node_name = central_store_util.get_node_name_from_id(
            node_id
        )
        return Response (
            NS.time_series_db_manager.get_plugin().get_metrics(node_name),
            status=200,
            mimetype='application/json'
        )
    except (
        ValueError,
        etcd.EtcdKeyNotFound,
        SyntaxError,
        TypeError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(
            str(ex),
            status=500,
            mimetype='application/json'
        )


@app.route("/monitoring/nodes/summary")
def get_node_summary():
    try:
        # Only 1 filter that is the node list is the only supported filter
        # anything else is simply ignored.
        summary = []
        ret_code = 200
        exs = ''
        is_filter = (
            len(request.args) == 1 and
            request.args.items()[0][0] == 'node_ids'
        )
        if is_filter:
            node_list = (request.args.items()[0][1]).split(",")
            for index, node in enumerate(node_list):
                uuid_string = node_list[index].strip()
                if UUID(
                    uuid_string,
                    version=4
                ).hex == uuid_string.replace('-', ''):
                    node_list[index] = node_list[index].strip()
                else:
                    raise TendrlPerformanceMonitoringException(
                        'Node id %s in the parameter is not a valid uuid' % (
                            uuid_string
                        )
                    )
            summary, ret_code, exs = \
                central_store_util.get_node_summary(node_list)
        else:
            summary, ret_code, exs = \
                central_store_util.get_node_summary()
        return Response(
            json.dumps(summary),
            status=ret_code,
            mimetype='application/json'
        )
    except (
        etcd.EtcdKeyNotFound,
        ValueError,
        SyntaxError,
        TendrlPerformanceMonitoringException,
        TypeError
    ) as ex:
        return Response(
            str(ex),
            status=500,
            mimetype='application/json'
        )


@app.route("/monitoring/nodes/<node_id>/iops/stats")
def get_nodeiopsstats(node_id):
    try:
        start_time = None
        end_time = None
        time_interval = None
        if len(request.args.items()) > 0:
            for request_param in request.args.items():
                if request_param[0] == "start_time":
                    start_time = request_param[1]
                elif request_param[0] == "end_time":
                    end_time = request_param[1]
                elif request_param[0] == "interval":
                    time_interval = request_param[1]
        return Response(
            NS.time_series_db_manager.get_plugin().\
            get_node_disk_iops_stats(
                node_id,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            ),
            status=200,
            mimetype='application/json'
        )
    except (
        ValueError,
        etcd.EtcdKeyNotFound,
        SyntaxError,
        urllib3.exceptions.HTTPError,
        TypeError,
        TendrlPerformanceMonitoringException
    ) as ex:
        return Response(str(ex), status=500, mimetype='application/json')


class TendrlPerformanceManager(object):

    def __init__(self):
        try:
            self.api_server = NS.performance_monitoring.config.data[
                'api_server_addr'
            ]
            self.api_port = int(
                NS.performance_monitoring.config.data[
                    'api_server_port'
                ]
            )
            NS.configurator_queue = multiprocessing.Queue()
            NS.sds_monitoring_manager = SDSMonitoringManager()
            self.configure_cluster_monitoring = ConfigureClusterMonitoring()
            self.node_summariser = NodeSummarise()
            self.cluster_summariser = ClusterSummarise()
            self.configure_node_monitoring = ConfigureNodeMonitoring()
        except (ConfigNotFound, TendrlPerformanceMonitoringException):
            raise

    def start(self):
        self.node_summariser.start()
        self.cluster_summariser.start()
        self.configure_cluster_monitoring.start()
        self.configure_node_monitoring.start()
        try:
            app.run(host=self.api_server, port=self.api_port, threaded=True)
        except (ValueError, TendrlPerformanceMonitoringException) as ex:
            Event(
                ExceptionMessage(
                    priority="debug",
                    publisher=NS.publisher_id,
                    payload={"message": 'Failed to start the api server.',
                             "exception": ex
                             }
                )
            )

            self.stop()

    def stop(self):
        # TODO (anmolB) central_store_thread is deprecated, move methods
        # inside it to respective utils
        self.configure_cluster_monitoring.stop()
        self.configure_node_monitoring.stop()
        NS.configurator_queue.close()
        self.node_summariser.stop()
        os.system("ps -C tendrl-performance-monitoring -o pid=|xargs kill -9")


def main():
    PerformanceMonitoringNS()
    TendrlNS()
    NS.publisher_id = "performance_monitoring"
    NS.time_series_db_manager = TimeSeriesDBManager()
    NS.performance_monitoring.definitions.save()
    NS.performance_monitoring.config.save()
    
    if NS.config.data.get("with_internal_profiling", False):
        from tendrl.commons import profiler
        profiler.start()
        
    tendrl_perf_manager = TendrlPerformanceManager()

    def terminate(sig, frame):
        Event(
            Message(
                priority="debug",
                publisher=NS.publisher_id,
                payload={"message": "Signal handler: stopping"}
            )
        )
        tendrl_perf_manager.stop()

    signal.signal(signal.SIGINT, terminate)
    tendrl_perf_manager.start()


if __name__ == "__main__":
    main()
