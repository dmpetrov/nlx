"""Manages logging configuration for DVC repo."""

import logging.config
import logging.handlers

import colorama

from dvc.progress import Tqdm

FOOTER = (
    "\n{yellow}Having any troubles?{nc}"
    " Hit us up at {blue}https://dvc.org/support{nc},"
    " we are always happy to help!"
).format(
    blue=colorama.Fore.BLUE,
    nc=colorama.Fore.RESET,
    yellow=colorama.Fore.YELLOW,
)


def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Adds a new logging level to the `logging` module and the
    currently configured logging class.

    Uses the existing numeric levelNum if already defined.

    Based on https://stackoverflow.com/questions/2183233
    """
    if methodName is None:
        methodName = levelName.lower()

    # If the level name is already defined as a top-level `logging`
    # constant, then adopt the existing numeric level.
    if hasattr(logging, levelName):
        existingLevelNum = getattr(logging, levelName)
        assert isinstance(existingLevelNum, int)
        levelNum = existingLevelNum

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            # pylint: disable=protected-access
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    # getLevelName resolves the numeric log level if already defined,
    # otherwise returns a string
    if not isinstance(logging.getLevelName(levelName), int):
        logging.addLevelName(levelNum, levelName)

    if not hasattr(logging, levelName):
        setattr(logging, levelName, levelNum)

    if not hasattr(logging.getLoggerClass(), methodName):
        setattr(logging.getLoggerClass(), methodName, logForLevel)

    if not hasattr(logging, methodName):
        setattr(logging, methodName, logToRoot)


class LoggingException(Exception):
    def __init__(self, record):
        msg = "failed to log {}".format(str(record))
        super().__init__(msg)


def excludeFilter(level):
    class ExcludeLevelFilter(logging.Filter):
        def filter(self, record):
            return record.levelno < level

    return ExcludeLevelFilter


class ColorFormatter(logging.Formatter):
    """Spit out colored text in supported terminals.

    colorama__ makes ANSI escape character sequences work under Windows.
    See the colorama documentation for details.

    __ https://pypi.python.org/pypi/colorama

    If record has an extra `tb_only` attribute, it will not show the
    exception cause, just the message and the traceback.
    """

    color_code = {
        "TRACE": colorama.Fore.GREEN,
        "DEBUG": colorama.Fore.BLUE,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "CRITICAL": colorama.Fore.RED,
    }

    def format(self, record):
        record.message = record.getMessage()
        msg = self.formatMessage(record)

        if record.levelname == "INFO":
            return msg

        if record.exc_info:
            if getattr(record, "tb_only", False):
                cause = ""
            else:
                cause = ": ".join(_iter_causes(record.exc_info[1]))

            msg = "{message}{separator}{cause}".format(
                message=msg or "",
                separator=" - " if msg and cause else "",
                cause=cause,
            )

            if _is_verbose():
                msg += _stack_trace(record.exc_info)

        return "{asctime}{color}{levelname}{nc}: {msg}".format(
            asctime=self.formatTime(record, self.datefmt),
            color=self.color_code[record.levelname],
            nc=colorama.Fore.RESET,
            levelname=record.levelname,
            msg=msg,
        )

    def formatTime(self, record, datefmt=None):
        # only show if current level is set to DEBUG
        # also, skip INFO as it is used for UI
        if not _is_verbose() or record.levelno == logging.INFO:
            return ""

        return "{green}{date}{nc} ".format(
            green=colorama.Fore.GREEN,
            date=super().formatTime(record, datefmt),
            nc=colorama.Fore.RESET,
        )


class LoggerHandler(logging.StreamHandler):
    def handleError(self, record):
        super().handleError(record)
        raise LoggingException(record)

    def emit(self, record):
        """Write to Tqdm's stream so as to not break progress-bars"""
        try:
            msg = self.format(record)
            Tqdm.write(
                msg, file=self.stream, end=getattr(self, "terminator", "\n")
            )
            self.flush()
        except RecursionError:
            raise
        except Exception:  # noqa, pylint: disable=broad-except
            self.handleError(record)


def _is_verbose():
    return (
        logging.NOTSET
        < logging.getLogger("dvc").getEffectiveLevel()
        <= logging.DEBUG
    )


def _iter_causes(exc):
    while exc:
        yield str(exc)
        exc = exc.__cause__


def _stack_trace(exc_info):
    import traceback

    return (
        "\n"
        "{red}{line}{nc}\n"
        "{trace}"
        "{red}{line}{nc}".format(
            red=colorama.Fore.RED,
            line="-" * 60,
            trace="".join(traceback.format_exception(*exc_info)),
            nc=colorama.Fore.RESET,
        )
    )


def disable_other_loggers():
    logging.captureWarnings(True)
    loggerDict = logging.root.manager.loggerDict  # pylint: disable=no-member
    for logger_name, logger in loggerDict.items():
        if logger_name != "dvc" and not logger_name.startswith("dvc."):
            logger.disabled = True


def setup(level=logging.INFO):
    colorama.init()

    if level >= logging.DEBUG:
        # Unclosed session errors for asyncio/aiohttp are only available
        # on the tracing mode for extensive debug purposes. They are really
        # noisy, and this is potentially somewhere in the client library
        # not managing their own session. Even though it is the best practice
        # for them to do so, we can be assured that these errors raised when
        # the object is getting deallocated, so no need to take any extensive
        # action.
        logging.getLogger("asyncio").setLevel(logging.CRITICAL)
        logging.getLogger("aiohttp").setLevel(logging.CRITICAL)

    addLoggingLevel("TRACE", logging.DEBUG - 5)
    logging.config.dictConfig(
        {
            "version": 1,
            "filters": {
                "exclude_errors": {"()": excludeFilter(logging.WARNING)},
                "exclude_info": {"()": excludeFilter(logging.INFO)},
                "exclude_debug": {"()": excludeFilter(logging.DEBUG)},
            },
            "formatters": {"color": {"()": ColorFormatter}},
            "handlers": {
                "console_info": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "INFO",
                    "formatter": "color",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_errors"],
                },
                "console_debug": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "DEBUG",
                    "formatter": "color",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_info"],
                },
                "console_trace": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "TRACE",
                    "formatter": "color",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_debug"],
                },
                "console_errors": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "WARNING",
                    "formatter": "color",
                    "stream": "ext://sys.stderr",
                },
            },
            "loggers": {
                "dvc": {
                    "level": level,
                    "handlers": [
                        "console_info",
                        "console_debug",
                        "console_trace",
                        "console_errors",
                    ],
                }
            },
            "disable_existing_loggers": False,
        }
    )
