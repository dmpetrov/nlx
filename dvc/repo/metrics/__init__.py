class Metrics:
    def __init__(self, repo):
        self.repo = repo

    def show(self, *args, **kwargs):
        from dvc.repo.metrics.show import show

        return show(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from .diff import diff

        return diff(self.repo, *args, **kwargs)
