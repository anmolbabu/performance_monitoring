from tendrl.commons import objects


class Thresholds(objects.BaseObject):
    def __init__(
        self,
        thresholds={},
        cluster_id=None,
        *args,
        **kwargs
    ):
        super(
            Thresholds,
            self
        ).__init__(*args, **kwargs)
        self.thresholds = thresholds
        self.cluster_id = cluster_id
        self.value = '/monitoring/config/thresholds/clusters/{0}'

    def render(self):
        self.value = self.value.format(self.cluster_id)
        return super(Thresholds, self).render()

    def save(self):
        old_thresholds = Thresholds(
            cluster_id=self.cluster_id
        ).loads()
        self.thresholds.update(old_thresholds.thresholds)
        super(Thresholds, self).save(update=False)
