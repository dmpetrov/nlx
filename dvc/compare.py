from collections import abc
from itertools import chain, repeat, zip_longest
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    ItemsView,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableSequence,
    Sequence,
    Tuple,
    Union,
    overload,
)

from funcy import reraise

if TYPE_CHECKING:
    from dvc.ui.table import CellT


class Column(List["CellT"]):
    pass


def with_value(value, default):
    return default if value is None else value


class TabularData(MutableSequence[Sequence["CellT"]]):
    def __init__(self, columns: Sequence[str], fill_value: str = ""):
        self._columns: Dict[str, Column] = {name: Column() for name in columns}
        self._keys: List[str] = list(columns)
        self._fill_value = fill_value

    @property
    def columns(self) -> List[Column]:
        return list(map(self.column, self.keys()))

    def column(self, name: str) -> Column:
        return self._columns[name]

    def items(self) -> ItemsView[str, Column]:
        projection = {k: self.column(k) for k in self.keys()}
        return projection.items()

    def keys(self) -> List[str]:
        return self._keys

    def _iter_col_row(
        self, row: Sequence["CellT"]
    ) -> Iterator[Tuple["CellT", Column]]:
        for val, col in zip_longest(row, self.columns):
            if col is None:
                break
            yield with_value(val, self._fill_value), col

    def append(self, value: Sequence["CellT"]) -> None:
        for val, col in self._iter_col_row(value):
            col.append(val)

    def extend(self, values: Iterable[Sequence["CellT"]]) -> None:
        for row in values:
            self.append(row)

    def insert(self, index: int, value: Sequence["CellT"]) -> None:
        for val, col in self._iter_col_row(value):
            col.insert(index, val)

    def __iter__(self) -> Iterator[List["CellT"]]:
        return map(list, zip(*self.columns))

    def __getattr__(self, item: str) -> Column:
        with reraise(KeyError, AttributeError):
            return self.column(item)

    def __getitem__(self, item: Union[int, slice]):
        func = itemgetter(item)
        it = map(func, self.columns)
        if isinstance(item, slice):
            it = map(list, zip(*it))
        return list(it)

    @overload
    def __setitem__(self, item: int, value: Sequence["CellT"]) -> None:
        ...

    @overload
    def __setitem__(
        self, item: slice, value: Iterable[Sequence["CellT"]]
    ) -> None:
        ...

    def __setitem__(self, item, value) -> None:
        it = value
        if isinstance(item, slice):
            n = len(self.columns)
            normalized_rows = (
                chain(val, repeat(self._fill_value, n - len(val)))
                for val in value
            )
            # we need to transpose those rows into columnar format
            # as we work in terms of column-based arrays
            it = zip(*normalized_rows)

        for i, col in self._iter_col_row(it):
            col[item] = i

    def __delitem__(self, item: Union[int, slice]) -> None:
        for col in self.columns:
            del col[item]

    def __len__(self) -> int:
        return len(self.columns[0])

    @property
    def shape(self) -> Tuple[int, int]:
        return len(self.columns), len(self)

    def drop(self, *col_names: str) -> None:
        for col_name in col_names:
            self._keys.remove(col_name)
            self._columns.pop(col_name)

    def rename(self, from_col_name: str, to_col_name: str) -> None:
        self._columns[to_col_name] = self._columns.pop(from_col_name)
        self._keys[self._keys.index(from_col_name)] = to_col_name

    def project(self, *col_names: str) -> None:
        self.drop(*(set(self._keys) - set(col_names)))
        self._keys = list(col_names)

    def to_csv(self) -> str:
        import csv
        from io import StringIO

        buff = StringIO()
        writer = csv.writer(buff)
        writer.writerow(self.keys())

        for row in self:
            writer.writerow(row)
        return buff.getvalue()

    def add_column(self, name: str) -> None:
        self._columns[name] = Column([self._fill_value] * len(self))
        self._keys.append(name)

    def row_from_dict(self, d: Mapping[str, "CellT"]) -> None:
        keys = self.keys()
        for key in d:
            if key not in keys:
                self.add_column(key)

        row: List["CellT"] = [
            with_value(d.get(key), self._fill_value) for key in self.keys()
        ]
        self.append(row)

    def render(self, **kwargs: Any):
        from dvc.ui import ui

        ui.table(self, headers=self.keys(), **kwargs)

    def as_dict(
        self, cols: Iterable[str] = None
    ) -> Iterable[Dict[str, "CellT"]]:
        keys = self.keys() if cols is None else set(cols)
        return [
            {k: self._columns[k][i] for k in keys} for i in range(len(self))
        ]


def _normalize_float(val: float, precision: int):
    return f"{val:.{precision}g}"


def _format_field(
    val: Any, precision: int = None, round_digits: bool = False
) -> str:
    def _format(_val):
        if isinstance(_val, float) and precision:
            func = round if round_digits else _normalize_float
            return func(_val, precision)
        if isinstance(_val, abc.Mapping):
            return {k: _format(v) for k, v in _val.items()}
        if isinstance(_val, list):
            return [_format(x) for x in _val]
        return _val

    return str(_format(val))


def diff_table(
    diff,
    title: str,
    old: bool = True,
    no_path: bool = False,
    show_changes: bool = True,
    precision: int = None,
    round_digits: bool = False,
    on_empty_diff: str = None,
) -> TabularData:
    headers: List[str] = ["Path", title, "Old", "New", "Change"]
    fill_value = "-"
    td = TabularData(headers, fill_value=fill_value)

    for fname, diff_in_file in diff.items():
        for item, change in sorted(diff_in_file.items()):
            old_value = with_value(change.get("old"), fill_value)
            new_value = with_value(change.get("new"), fill_value)
            diff_value = with_value(
                change.get("diff", on_empty_diff), fill_value
            )
            td.append(
                [
                    fname,
                    str(item),
                    _format_field(old_value, precision, round_digits),
                    _format_field(new_value, precision, round_digits),
                    _format_field(diff_value, precision, round_digits),
                ]
            )

    if no_path:
        td.drop("Path")

    if not show_changes:
        td.drop("Change")

    if not old:
        td.drop("Old")
        td.rename("New", "Value")

    return td


def show_diff(
    diff,
    title: str,
    old: bool = True,
    no_path: bool = False,
    show_changes: bool = True,
    precision: int = None,
    round_digits: bool = False,
    on_empty_diff: str = None,
    markdown: bool = False,
) -> None:
    td = diff_table(
        diff,
        title=title,
        old=old,
        no_path=no_path,
        show_changes=show_changes,
        precision=precision,
        round_digits=round_digits,
        on_empty_diff=on_empty_diff,
    )
    td.render(markdown=markdown)


def metrics_table(
    metrics,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    precision: int = None,
    round_digits: bool = False,
):
    from dvc.utils.diff import format_dict
    from dvc.utils.flatten import flatten

    td = TabularData(["Revision", "Path"], fill_value="-")

    for branch, val in metrics.items():
        for fname, metric in val.items():
            row_data: Dict[str, str] = {"Revision": branch, "Path": fname}
            flattened = (
                flatten(format_dict(metric))
                if isinstance(metric, dict)
                else {"": metric}
            )
            row_data.update(
                {
                    k: _format_field(v, precision, round_digits)
                    for k, v in flattened.items()
                }
            )
            td.row_from_dict(row_data)

    rev, path, *metrics_headers = td.keys()
    td.project(rev, path, *sorted(metrics_headers))

    if not any([all_branches, all_tags, all_commits]):
        td.drop("Revision")

    return td


def show_metrics(
    metrics,
    markdown: bool = False,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    precision: int = None,
    round_digits: bool = False,
) -> None:
    td = metrics_table(
        metrics,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        precision=precision,
        round_digits=round_digits,
    )
    td.render(markdown=markdown)
