import os


def get_url(url, out=None, jobs=None):
    from dvc import dependency, output
    from dvc.utils import resolve_output

    out = resolve_output(url, out)

    if os.path.exists(url):
        url = os.path.abspath(url)

    out = os.path.abspath(out)

    (dep,) = dependency.loads_from(None, [url])
    (out,) = output.loads_from(None, [out], use_cache=False)
    if not dep.exists:
        raise dep.DoesNotExistError(dep)
    dep.download(out, jobs=jobs)
