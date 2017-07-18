from etcd import EtcdException
from etcd import EtcdKeyNotFound
import json
from ruamel import yaml
import urllib3
from tendrl.commons.event import Event
from tendrl.commons.message import ExceptionMessage
from tendrl.performance_monitoring.objects.cluster_summary \
    import ClusterSummary
from tendrl.performance_monitoring.exceptions \
    import TendrlPerformanceMonitoringException
from tendrl.performance_monitoring.objects.system_summary \
    import SystemSummary
from tendrl.performance_monitoring import constants as \
    pm_consts


def read_key(key):
    try:
        return NS._int.client.read(key, quorum=True)
    except (AttributeError, EtcdException) as ex:
        if type(ex) == EtcdKeyNotFound:
            raise ex
        else:
            try:
                NS._int.reconnect()
                return NS._int.client.read(key, quorum=True)
            except (AttributeError, EtcdException) as ex:
                raise ex


# this function can return json for any etcd key
def read(key):
    result = {}
    job = {}
    try:
        job = read_key(key)
    except EtcdKeyNotFound:
        pass
    except (AttributeError, EtcdException) as ex:
        raise ex
    if hasattr(job, 'leaves'):
        for item in job.leaves:
            if key == item.key:
                result[item.key.split("/")[-1]] = item.value
                return result
            if item.dir is True:
                result[item.key.split("/")[-1]] = read(item.key)
            else:
                result[item.key.split("/")[-1]] = item.value
    return result


def get_configs():
    # TODO(Anmol) : Attempt reading:
    # /_tendrl/config/performance_monitoring/clusters/{integration-id} and if
    # not already present, default back to defaults in:
    #  /_tendrl/config/performance_monitoring
    try:
        configs = ''
        conf = read_key(
            '_NS/performance_monitoring/config'
        )
        configs = conf.value
        return yaml.safe_load(configs)
    except (
        EtcdException,
        ValueError,
        SyntaxError,
        AttributeError
    ) as ex:
        Event(
            ExceptionMessage(
                priority="debug",
                publisher=NS.publisher_id,
                payload={
                    "message": 'Fetching monitoring configurations failed.',
                         "exception": ex
                }
            )
        )
        raise ex


def get_node_last_seen_at(node_id):
    try:
        return read_key(
            '/monitoring/nodes/%s/last_seen_at' % node_id
        ).value
    except EtcdKeyNotFound:
        return None


def get_node_name_from_id(node_id):
    try:
        node_name_path = '/nodes/%s/NodeContext/fqdn' % node_id
        return read_key(node_name_path).value
    except (
        AttributeError,
        ValueError,
        SyntaxError,
        EtcdException,
        TypeError
    ) as ex:
        raise ex


def get_node_role(node_id):
    try:
        return read_key(
            '/nodes/%s/NodeContext/tags' % node_id
        ).value
    except (EtcdException, AttributeError) as ex:
        raise ex


def get_node_cluster_name(node_id):
    try:
        return read_key(
            '/nodes/%s/TendrlContext/cluster_name' % node_id
        ).value
    except (AttributeError, EtcdException) as ex:
        raise ex


def get_integration_ids():
    try:
        integration_ids = []
        clusters_etcd = read_key('/clusters')
        for cluster in clusters_etcd.leaves:
            cluster_key_contents = cluster.key.split('/')
            if len(cluster_key_contents) == 3:
                integration_ids.append(cluster_key_contents[2])
        return integration_ids
    except EtcdKeyNotFound:
        return []
    except (
        AttributeError,
        EtcdException,
        ValueError,
        SyntaxError,
        TypeError
    ) as ex:
        raise ex


def get_node_ids():
    try:
        node_ids = []
        nodes_etcd = read_key('/nodes')
        for node in nodes_etcd.leaves:
            node_key_contents = node.key.split('/')
            if len(node_key_contents) == 3:
                node_ids.append(node_key_contents[2])
        return node_ids
    except EtcdKeyNotFound:
        return []
    except (
        AttributeError,
        EtcdException,
        ValueError,
        SyntaxError,
        TypeError
    ) as ex:
        raise ex


def get_node_alert_ids(node_id=None):
    alert_ids = []
    try:
        alerts = read_key(
            '/alerting/nodes/%s' % node_id
        )
        for alert in alerts.leaves:
            key_contents = alert.key.split('/')
            if len(key_contents) == 5:
                alert_ids.append(
                    key_contents[4]
                )
    except EtcdKeyNotFound as ex:
        return alert_ids
    except (
        AttributeError,
        EtcdException
    ) as ex:
        raise ex
    return alert_ids


def get_node_alerts(node_id):
    alert_root = '/alerting/nodes/%s' % node_id
    node_alerts_arr = []
    try:
        node_alerts = read(alert_root)
        for alert_id, node_alert in node_alerts.iteritems():
            node_alerts_arr.append(node_alert)
    except EtcdKeyNotFound:
        pass
    except (AttributeError, EtcdException) as ex:
        raise ex
    return node_alerts_arr


def get_cluster_alerts(integration_id):
    cluster_alerts = []
    try:
        c_alerts = read(
            '/alerting/clusters/%s' % integration_id
        )
        for alert_id, alert in c_alerts.iteritems():
            cluster_alerts.append(alert)
        return cluster_alerts
    except EtcdKeyNotFound:
        return cluster_alerts


def get_cluster_summary(integration_id):
    try:
        summary = ClusterSummary(
            integration_id=integration_id
        )
        if not summary.exists():
            raise EtcdException(
                "No summary found for cluster %s" % integration_id
            )
        summary = summary.load().to_json()
        for key, value in summary.items():
            if (
                key.startswith("_") or
                key in ['hash', 'updated_at', 'value', 'list']
            ):
                del summary[key]
        return summary
    except (EtcdKeyNotFound, AttributeError, TypeError, ValueError) as ex:
        raise ex


def get_system_summary(cluster_type):
    try:
        summary = SystemSummary(
            sds_type=cluster_type
        )
        if not summary.exists():
            raise EtcdException(
                "No clusters of type %s found" % cluster_type
            )
        summary = summary.load().to_json()
        for key, value in summary.items():
            if (
                key.startswith("_") or
                key in ['hash', 'updated_at', 'value', 'list']
            ):
                del summary[key]
        return summary
    except (EtcdKeyNotFound, AttributeError, TypeError, ValueError) as ex:
        raise ex


def get_node_summary(node_ids=None):
    summary = []
    exs = ''
    if node_ids is None:
        node_ids = get_node_ids()
    for node_id in node_ids:
        try:
            current_node_summary = read(
                '/monitoring/summary/nodes/%s' % node_id
            )
            for key, value in current_node_summary.items():
                if (
                    key.startswith("_") or
                    key in ['hash', 'updated_at', 'value', 'list']
                ):
                    del current_node_summary[key]
            summary.append(current_node_summary)
        except (EtcdKeyNotFound, AttributeError, TypeError, ValueError):
            exs = "%s.Failed to fetch summary for node with id: %s" % (
                exs,
                node_id
            )
            continue
    if len(summary) == len(node_ids):
        return summary, 200, None
    else:
        if len(summary) == 0:
            return summary, 500, exs
        else:
            return summary, 206, exs


def get_cluster_iops(
    integration_ids=None,
    time_interval=None,
    start_time=None,
    end_time=None
):
    iops = []
    exs = ''
    if integration_ids is None:
        integration_ids = get_integration_ids()
    for integration_id in integration_ids:
        try:
            entity_name, metric_name = NS.time_series_db_manager.\
                get_timeseriesnamefromresource(
                    integration_id=integration_id,
                    resource_name=pm_consts.IOPS,
                    utilization_type=pm_consts.TOTAL
                ).split(
                    NS.time_series_db_manager.get_plugin().get_delimeter(),
                    1
                )
            read_key('/clusters/%s' % integration_id)
            cluster_iops = NS.time_series_db_manager.get_plugin(
            ).get_metric_stats(
                entity_name,
                metric_name,
                time_interval=time_interval,
                start_time=start_time,
                end_time=end_time
            )
            json_io = json.loads(cluster_iops)
            if (
                isinstance(json_io, list) and
                len(json_io) > 0
            ):
                json_io[0]['integration_id'] = integration_id
                iops.append(json_io[0])
            else:
                json_io = {
                    'integration_id': integration_id,
                    'target': '',
                    'datapoints': []
                }
                iops.append(json_io)
        except (
            ValueError,
            EtcdException,
            AttributeError,
            urllib3.exceptions.HTTPError,
            TendrlPerformanceMonitoringException
        ) as ex:
            exs = "%s.Failed to fetch iops of cluster with id: %s.Error %s" % (
                exs,
                integration_id,
                str(ex)
            )
            continue
    if len(iops) == len(integration_ids):
        return iops, 200, None
    else:
        if len(iops) == 0:
            return iops, 500, exs
        else:
            return iops, 206, exs


def get_nodes_details():
    nodes_dets = []
    try:
        nodes = read_key('/nodes/')
        for node in nodes.leaves:
            if node.key.startswith('/nodes/'):
                node_id = (
                    node.key.split('/')[2]
                ).encode('ascii', 'ignore')
                fqdn = (
                    read_key(
                        '/nodes/%s/NodeContext/fqdn' % (node_id)
                    ).value
                ).encode('ascii', 'ignore')
                nodes_dets.append({'node_id': node_id, 'fqdn': fqdn})
        return nodes_dets
    except EtcdKeyNotFound:
        return nodes_dets
    except EtcdException as ex:
        raise ex


def get_node_selinux_mode(node_id):
    return read_key(
        'nodes/%s/Os/selinux_mode' % node_id
    ).value


def get_cluster_node_ids(integration_id):
    cluster_nodes = []
    try:
        nodes = read_key(
            '/clusters/%s/nodes' % integration_id
        )
        for node in nodes.leaves:
            key_contents = node.key.split('/')
            if len(key_contents) == 5:
                cluster_nodes.append(key_contents[4])
        return cluster_nodes
    except EtcdKeyNotFound:
        return cluster_nodes


def get_cluster_name(integration_id):
    return read_key(
        '/clusters/%s/TendrlContext/cluster_name' % integration_id
    ).value


def get_volume_name(integration_id, vol_id):
    return read_key(
        '/clusters/%s/Volumes/%s/name' % (
            integration_id,
            vol_id
        )
    ).value


def get_node_sds_name(node_id):
    sds_name = ''
    try:
        sds_name = read_key(
            '/nodes/%s/TendrlContext/sds_name' % node_id
        ).value
    except (EtcdException, AttributeError):
        pass
    return sds_name


def get_node_integration_id(node_id):
    integration_id = ''
    try:
        integration_id = read_key(
            '/nodes/%s/TendrlContext/integration_id' % node_id
        ).value
    except (AttributeError, EtcdException):
        pass
    return integration_id


def get_cluster_node_contexts(integration_id):
    node_contexts = {}
    node_ids = get_cluster_node_ids(integration_id)
    for node_id in node_ids:
        try:
            node_context = read(
                '/nodes/%s/NodeContext' % node_id
            )
            node_contexts[node_id] = node_context
        except EtcdKeyNotFound:
            continue
    return node_contexts


def get_node_names_in_cluster(integration_id):
    ret_val = []
    nodes = read('/clusters/%s/nodes' % integration_id)
    for node_id, node_det in nodes.iteritems():
        ret_val.append(node_det.get('NodeContext', {}).get('fqdn'))
    return ret_val


def get_cluster_sds_name(integration_id):
    return read_key(
        '/clusters/%s/TendrlContext/sds_name' % integration_id
    ).value
