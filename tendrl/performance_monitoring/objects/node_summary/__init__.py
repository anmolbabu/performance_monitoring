from tendrl.commons.etcdobj import EtcdObj
from tendrl.commons.objects import BaseObject


class NodeSummary(BaseObject):
    def __init__(
        self,
        name=None,
        node_id=None,
        status=None,
        role=None,
        cluster_name=None,
        cpu_usage=None,
        memory_usage=None,
        storage_usage=None,
        swap_usage=None,
        alert_count=None,
        sds_det=None,
        selinux_mode='',
        *args,
        **kwargs
    ):
        super(NodeSummary, self).__init__(*args, **kwargs)
        self.node_id = node_id
        self.value = 'monitoring/summary/nodes/%s' % self.node_id
        self._etcd_cls = _NodeSummaryEtcd
        if cpu_usage is not None:
            self.cpu_usage = cpu_usage
        if memory_usage is not None:
            self.memory_usage = memory_usage
        if storage_usage is not None:
            self.storage_usage = storage_usage
        if swap_usage is not None:
            self.swap_usage = swap_usage
        self.name = name
        self.status = status
        self.role = role
        self.cluster_name = cluster_name
        self.selinux_mode = selinux_mode
        self.sds_det = sds_det
        self.alert_count = alert_count

    def to_json(self):
        return self.__dict__


class _NodeSummaryEtcd(EtcdObj):
    """A table of the node summary, lazily updated

    """
    __name__ = 'monitoring/summary/nodes/%s'
    _tendrl_cls = NodeSummary

    def render(self):
        self.__name__ = self.__name__ % self.node_id
        return super(_NodeSummaryEtcd, self).render()
