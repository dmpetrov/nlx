# Path Specification Pattern Math
# Including changing base dir of path specification patterns and merging
# of two path specification patterns with different base
# All the operations follow the documents of `gitignore`
import os
from collections import namedtuple

from pathspec.util import normalize_file

from dvc.utils import relpath

PatternInfo = namedtuple("PatternInfo", ["patterns", "file_info"])


def _not_ignore(rule):
    return (True, rule[1:]) if rule.startswith("!") else (False, rule)


def _is_comment(rule):
    return rule.startswith("#")


def _remove_slash(rule):
    if rule.startswith("\\"):
        return rule[1:]
    return rule


def _match_all_level(rule):
    if rule[:-1].find("/") >= 0 and not rule.startswith("**/"):
        if rule.startswith("/"):
            rule = rule[1:]
        return False, rule
    if rule.startswith("**/"):
        rule = rule[3:]
    return True, rule


def change_rule(rule, rel):
    rule = rule.strip()
    if _is_comment(rule):
        return rule
    not_ignore, rule = _not_ignore(rule)
    match_all, rule = _match_all_level(rule)
    rule = _remove_slash(rule)
    if not match_all:
        rule = f"/{rule}"
    else:
        rule = f"/**/{rule}"
    if not_ignore:
        rule = f"!/{rel}{rule}"
    else:
        rule = f"/{rel}{rule}"
    rule = normalize_file(rule)
    return rule


def _change_dirname(dirname, pattern_list, new_dirname):
    if new_dirname == dirname:
        return pattern_list
    rel = relpath(dirname, new_dirname)
    if rel.startswith(".."):
        raise ValueError("change dirname can only change to parent path")

    return [
        PatternInfo(change_rule(rule.patterns, rel), rule.file_info)
        for rule in pattern_list
    ]


def merge_patterns(pattern_a, prefix_a, pattern_b, prefix_b):
    """
    Merge two path specification patterns.

    This implementation merge two path specification patterns on different
    bases. It returns the longest common parent directory, and the patterns
    based on this new base directory.
    """
    if not pattern_a:
        return pattern_b, prefix_b
    if not pattern_b:
        return pattern_a, prefix_a

    longest_common_dir = os.path.commonpath([prefix_a, prefix_b])
    new_pattern_a = _change_dirname(prefix_a, pattern_a, longest_common_dir)
    new_pattern_b = _change_dirname(prefix_b, pattern_b, longest_common_dir)

    if len(prefix_a) <= len(prefix_b):
        merged_pattern = new_pattern_a + new_pattern_b
    else:
        merged_pattern = new_pattern_b + new_pattern_a

    return merged_pattern, longest_common_dir
