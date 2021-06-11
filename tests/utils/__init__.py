import csv
import os
from contextlib import contextmanager

from funcy import first

from dvc.scm import Git


def get_gitignore_content():
    with open(Git.GITIGNORE) as gitignore:
        return gitignore.read().splitlines()


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


def to_posixpath(path):
    return path.replace("\\", "/")


def dump_sv(stream, metrics, delimiter=",", header=True):
    if header:
        writer = csv.DictWriter(
            stream, fieldnames=list(first(metrics).keys()), delimiter=delimiter
        )
        writer.writeheader()
        writer.writerows(metrics)
    else:
        writer = csv.writer(stream)
        for d in metrics:
            writer.writerow(list(d.values()))
