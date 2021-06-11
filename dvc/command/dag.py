import argparse

from dvc.command.base import CmdBase, append_doc_link
from dvc.ui import ui


def _show_ascii(G):
    from dvc.dagascii import draw
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(G)

    ret = []
    for pipeline in pipelines:
        ret.append(draw(pipeline.nodes, pipeline.edges))

    return "\n".join(ret)


def _show_dot(G):
    import io

    from networkx.drawing.nx_pydot import write_dot

    dot_file = io.StringIO()
    write_dot(G, dot_file)
    return dot_file.getvalue()


def _collect_targets(repo, target, outs):
    if not target:
        return []

    pairs = repo.stage.collect_granular(target)
    if not outs:
        return [stage.addressing for stage, _ in pairs]

    targets = []
    for stage, info in pairs:
        if not info:
            targets.extend([str(out) for out in stage.outs])
            continue

        for out in repo.outs_trie.itervalues(prefix=info.parts):  # noqa: B301
            targets.extend(str(out))

    return targets


def _transform(repo, outs):
    import networkx as nx

    from dvc.stage import Stage

    def _relabel(node) -> str:
        return node.addressing if isinstance(node, Stage) else str(node)

    G = repo.outs_graph if outs else repo.graph
    return nx.relabel_nodes(G, _relabel, copy=True)


def _filter(G, targets, full):
    import networkx as nx

    if not targets:
        return G

    H = G.copy()
    if not full:
        descendants = set()
        for target in targets:
            descendants.update(nx.descendants(G, target))
            descendants.add(target)
        H.remove_nodes_from(set(G.nodes()) - descendants)

    undirected = H.to_undirected()
    connected = set()
    for target in targets:
        connected.update(nx.node_connected_component(undirected, target))

    H.remove_nodes_from(set(H.nodes()) - connected)

    return H


def _build(repo, target=None, full=False, outs=False):
    targets = _collect_targets(repo, target, outs)
    G = _transform(repo, outs)
    return _filter(G, targets, full)


class CmdDAG(CmdBase):
    def run(self):
        G = _build(
            self.repo,
            target=self.args.target,
            full=self.args.full,
            outs=self.args.outs,
        )

        if self.args.dot:
            ui.write(_show_dot(G))
        else:
            from dvc.utils.pager import pager

            pager(_show_ascii(G))

        return 0


def add_parser(subparsers, parent_parser):
    DAG_HELP = "Visualize DVC project DAG."
    dag_parser = subparsers.add_parser(
        "dag",
        parents=[parent_parser],
        description=append_doc_link(DAG_HELP, "dag"),
        help=DAG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    dag_parser.add_argument(
        "--dot",
        action="store_true",
        default=False,
        help="Print DAG with .dot format.",
    )
    dag_parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help=(
            "Show full DAG that the target belongs too, instead of "
            "showing DAG consisting only of ancestors."
        ),
    )
    dag_parser.add_argument(
        "-o",
        "--outs",
        action="store_true",
        default=False,
        help="Print output files instead of stages.",
    )
    dag_parser.add_argument(
        "target",
        nargs="?",
        help="Stage or output to show pipeline for (optional). "
        "Finds all stages in the workspace by default.",
    )
    dag_parser.set_defaults(func=CmdDAG)
