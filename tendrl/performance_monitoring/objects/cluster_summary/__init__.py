import json
from tendrl.commons import objects
from tendrl.performance_monitoring import constants as \
    pm_consts


class ClusterSummary(objects.BaseObject):
    def __init__(self,
                 utilization={'': ''},
                 iops=pm_consts.NOT_AVAILABLE,
                 hosts_count={'': ''},
                 node_summaries=[],
                 sds_det={'': ''},
                 sds_type='',
                 integration_id='',
                 *args,
                 **kwargs
                 ):
        super(
            ClusterSummary,
            self
        ).__init__(*args, **kwargs)
        self.integration_id = integration_id
        self.utilization = utilization
        self.iops = iops
        self.hosts_count = hosts_count
        self.node_summaries = node_summaries
        self.sds_det = sds_det
        self.sds_type = sds_type
        self.value = 'monitoring/summary/clusters/{0}'

    def to_json(self):
        # TODO (anmolB) Use self.json instead of this method
        return self.__dict__

    def save(self, update=False):
        # Convert nested dict to str @ save and convert back to
        # dict on load
        self.node_summaries = json.dumps(self.node_summaries)
        self.hosts_count = json.dumps(self.hosts_count)
        self.utilization = json.dumps(self.utilization)
        self.sds_det = json.dumps(self.sds_det)
        super(ClusterSummary, self).save(update=update)

    def load(self):
        summary = super(ClusterSummary, self).load()
        if isinstance(summary.node_summaries, basestring):
            summary.node_summaries = json.loads(summary.node_summaries)
        summary.sds_det = json.loads(summary.sds_det[''])
        summary.hosts_count = json.loads(summary.hosts_count[''])
        summary.utilization = json.loads(summary.utilization[''])
        return summary

    def copy(self):
        return ClusterSummary(
            utilization=self.utilization,
            iops=self.iops,
            hosts_count=self.hosts_count,
            node_summaries=self.node_summaries,
            sds_det=self.sds_det,
            sds_type=self.sds_type,
            integration_id=self.integration_id
        )

    def render(self):
        self.value = self.value.format(self.integration_id)
        return super(ClusterSummary, self).render()
