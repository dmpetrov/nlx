import logging
import time
import traceback
from datetime import datetime

import colorama
import pytest

import dvc.logger
from dvc.exceptions import DvcException

logger = logging.getLogger("dvc")
formatter = dvc.logger.ColorFormatter()
colors = {
    "blue": colorama.Fore.BLUE,
    "green": colorama.Fore.GREEN,
    "red": colorama.Fore.RED,
    "yellow": colorama.Fore.YELLOW,
    "nc": colorama.Fore.RESET,
}


@pytest.fixture()
def dt(mocker):
    mocker.patch(
        "time.time", return_value=time.mktime(datetime(2020, 2, 2).timetuple())
    )
    yield "2020-02-02 00:00:00,000"


class TestColorFormatter:
    # pylint: disable=broad-except
    def test_debug(self, caplog, dt):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            logger.debug("message")

            expected = "{green}{datetime}{nc} {blue}DEBUG{nc}: message".format(
                **colors, datetime=dt
            )

            assert expected == formatter.format(caplog.records[0])

    def test_info(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.info("message")

            assert "message" == formatter.format(caplog.records[0])

    def test_warning(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.warning("message")

            expected = "{yellow}WARNING{nc}: message".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_error(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.error("message")

            expected = "{red}ERROR{nc}: message".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_exception(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise ValueError
            except Exception:
                logger.exception("message")

            expected = "{red}ERROR{nc}: message".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_exception_with_description_and_without_message(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                logger.exception("")

            expected = "{red}ERROR{nc}: description".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_exception_with_description_and_message(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                logger.exception("message")

            expected = "{red}ERROR{nc}: message - description".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_exception_under_verbose(self, caplog, dt):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                stack_trace = traceback.format_exc()
                logger.exception("")

            expected = (
                "{green}{datetime}{nc} "
                "{red}ERROR{nc}: description\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}".format(
                    line="-" * 60,
                    stack_trace=stack_trace,
                    **colors,
                    datetime=dt,
                )
            )

            assert expected == formatter.format(caplog.records[0])

    def test_exc_info_on_other_record_types(self, caplog, dt):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                stack_trace = traceback.format_exc()
                logger.debug("", exc_info=True)

            expected = (
                "{green}{datetime}{nc} "
                "{blue}DEBUG{nc}: description\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}".format(
                    line="-" * 60,
                    stack_trace=stack_trace,
                    datetime=dt,
                    **colors,
                )
            )

            assert expected == formatter.format(caplog.records[0])

    def test_tb_only(self, caplog, dt):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                stack_trace = traceback.format_exc()
                logger.exception("something", extra={"tb_only": True})

            expected = (
                "{green}{datetime}{nc} "
                "{red}ERROR{nc}: something\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}".format(
                    line="-" * 60,
                    stack_trace=stack_trace,
                    **colors,
                    datetime=dt,
                )
            )

            assert expected == formatter.format(caplog.records[0])

    def test_nested_exceptions(self, caplog, dt):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("first")
            except Exception as exc:
                try:
                    raise DvcException("second") from exc
                except DvcException:
                    stack_trace = traceback.format_exc()
                    logger.exception("message")

            expected = (
                "{green}{datetime}{nc} "
                "{red}ERROR{nc}: message - second: first\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}".format(
                    line="-" * 60,
                    stack_trace=stack_trace,
                    **colors,
                    datetime=dt,
                )
            )
            assert expected == formatter.format(caplog.records[0])
            assert "Exception: first" in stack_trace
            assert "dvc.exceptions.DvcException: second" in stack_trace

    def test_progress_awareness(self, mocker, capsys, caplog):
        from dvc.progress import Tqdm

        mocker.patch("sys.stdout.isatty", return_value=True)
        with Tqdm(total=100, desc="progress") as pbar:
            pbar.update()

            # logging an invisible message should not break
            # the progress bar output
            with caplog.at_level(logging.INFO, logger="dvc"):
                debug_record = logging.LogRecord(
                    name="dvc",
                    level=logging.DEBUG,
                    pathname=__name__,
                    lineno=1,
                    msg="debug",
                    args=(),
                    exc_info=None,
                )

                formatter.format(debug_record)
                captured = capsys.readouterr()
                assert captured.out == ""

            #  when the message is actually visible
            with caplog.at_level(logging.INFO, logger="dvc"):
                logger.info("some info")
                captured = capsys.readouterr()
                assert captured.out == ""


def test_handlers():
    out, deb, vrb, err = logger.handlers

    assert out.level == logging.INFO
    assert deb.level == logging.DEBUG
    assert vrb.level == logging.TRACE
    assert err.level == logging.WARNING


def test_logging_debug_with_datetime(caplog, dt):
    with caplog.at_level(logging.DEBUG, logger="dvc"):
        logger.warning("WARNING")
        logger.debug("DEBUG")
        logger.trace("TRACE")
        logger.error("ERROR")

        for record in caplog.records:
            assert dt in formatter.format(record)
            assert record.levelname == record.message


def test_info_with_debug_loglevel_shows_no_datetime(caplog, dt):
    with caplog.at_level(logging.DEBUG, logger="dvc"):
        logger.info("message")

        assert "message" == formatter.format(caplog.records[0])


def test_add_existing_level(caplog, dt):
    # Common pattern to configure logging level in external libraries
    # eg:
    # https://github.com/bokeh/bokeh/blob/04bb30fef2e72e64baaa8b2f330806d5bfdd3b11/
    # bokeh/util/logconfig.py#L79-L85
    TRACE2 = 4
    logging.addLevelName(TRACE2, "TRACE2")
    logging.TRACE2 = TRACE2

    dvc.logger.addLoggingLevel("TRACE2", 2)

    # DVC sets all expected entrypoints, but doesn't override the level
    assert logging.TRACE2 == 4
    assert hasattr(logging, "trace2")
    assert hasattr(logger, "trace2")
    assert logging.getLevelName("TRACE2") == 4

    # The TRACE2 logging level uses the original, higher logging level
    with caplog.at_level(logging.TRACE2, logger="dvc"):
        logger.trace2("TRACE2")
    assert len(caplog.records) == 1

    (record,) = caplog.records
    assert record.levelno == 4
    assert record.levelname == "TRACE2"
    assert record.message == "TRACE2"
