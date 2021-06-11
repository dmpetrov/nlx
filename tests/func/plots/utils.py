import json

from tests.utils import dump_sv


def _write_csv(metric, filename, header=True):
    with open(filename, "w", newline="") as csvobj:
        dump_sv(csvobj, metric, delimiter=",", header=header)


def _write_json(tmp_dir, metric, filename):
    tmp_dir.gen(filename, json.dumps(metric, sort_keys=True))
