from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from argparse import Namespace


@contextmanager
def instrument(html_output=False):
    """Run a statistical profiler"""
    try:
        from pyinstrument import Profiler  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, pyinstrument is not installed")
        yield
        return

    profiler = Profiler()

    profiler.start()
    yield
    profiler.stop()

    if html_output:
        profiler.open_in_browser()
        return
    print(profiler.output_text(unicode=True, color=True))


@contextmanager
def profile(dump_path: str = None):
    """Run a cprofile"""
    import cProfile

    prof = cProfile.Profile()
    prof.enable()

    yield

    prof.disable()
    if not dump_path:
        prof.print_stats(sort="cumtime")
        return
    prof.dump_stats(dump_path)


@contextmanager
def debug():
    try:
        yield
    except Exception:  # pylint: disable=broad-except
        try:
            import ipdb as pdb  # noqa: T100, pylint: disable=import-error
        except ImportError:
            import pdb  # noqa: T100
        pdb.post_mortem()

        raise  # prevent from jumping ahead


@contextmanager
def debugtools(args: "Namespace" = None, **kwargs):
    kw = vars(args) if args else {}
    kw.update(kwargs)

    with ExitStack() as stack:
        if kw.get("pdb"):
            stack.enter_context(debug())
        if kw.get("cprofile") or kw.get("cprofile_dump"):
            stack.enter_context(profile(kw.get("cprofile_dump")))
        if kw.get("instrument") or kw.get("instrument_open"):
            stack.enter_context(instrument(kw.get("instrument_open", False)))
        yield


def add_debugging_flags(parser):
    from argparse import SUPPRESS

    parser.add_argument(
        "--cprofile", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument("--cprofile-dump", help=SUPPRESS)
    parser.add_argument(
        "--pdb", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--instrument", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--instrument-open", action="store_true", default=False, help=SUPPRESS
    )
