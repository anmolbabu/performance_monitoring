import copy
from tendrl.performance_monitoring.objects.thresholds import Thresholds
import tendrl.performance_monitoring.utils.central_store_util as \
    central_store_util


class UtililizationStates(object):
    WARNING = "warning"
    CRITICAL = "critical"
    INFO = "info"


class UtilizationTypes(object):
    CPU = 'cpu'
    MEMORY = 'memory'
    MOUNT_POINT = 'mount_point'
    SWAP = 'swap'
    CLUSTER_UTILIZATION = 'cluster_utilization'


def get_thresholds(integration_id=None, sds_name=None, node_id=None):
    if node_id:
        integration_id = central_store_util.get_node_cluster_id(node_id)
    if not integration_id:
        return NS.performance_monitoring.config.data['thresholds']['node']
    thresholds = Thresholds(
        integration_id=integration_id
    )
    if (
        not thresholds or
        not thresholds.exists()
    ):
        configs = NS.performance_monitoring.config.data['thresholds']
        # Strip wrapper tags like node or sds-type
        t_configs = copy.deepcopy(configs.get('node'))
        if sds_name:
            t_configs.update(copy.deepcopy(configs.get(sds_name)))
        _set_thresholds(
            integration_id,
            NS.performance_monitoring.config.data['thresholds']
        )
    return thresholds.load().thresholds


def get_utilization_state(
    utilization_type,
    utilization_val,
    integration_id=None,
    sds_name=None,
    node_id=None
):
    thresholds = get_thresholds(integration_id, node_id)
    state = UtililizationStates.INFO
    for u_type, threshold_vals in thresholds.iteritems():
        if u_type == utilization_type:
            if utilization_val > threshold_vals['Warning']:
                state = UtililizationStates.WARNING
            if utilization_val > threshold_vals['Failure']:
                state = UtililizationStates.CRITICAL
            break
    return state


def _set_thresholds(cluster_id, threshold_vals):
    thresholds = Thresholds(
        thresholds=threshold_vals,
        cluster_id=cluster_id
    )
    thresholds.save()


def enforce_thresholds(cluster_id, thresholds):
    set_thresholds(cluster_id, thresholds)
    # TODO(anbabu): Code to actually write new thresholds goes here..
    # The complete implementation of this function adds capabilties to
    # override default configured thresholds.
