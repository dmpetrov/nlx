import asyncio
import logging
import os
import sys
import threading
from contextlib import contextmanager

from fsspec.utils import infer_storage_options
from funcy import cached_property, memoize, wrap_prop

from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes
from dvc.utils import format_link

from .fsspec_wrapper import ObjectFSWrapper

logger = logging.getLogger(__name__)
_DEFAULT_CREDS_STEPS = (
    "https://azuresdkdocs.blob.core.windows.net/$web/python/"
    "azure-identity/1.4.0/azure.identity.html#azure.identity"
    ".DefaultAzureCredential"
)


@contextmanager
def _temp_event_loop():
    """When trying to initalize azure filesystem instances
    with DefaultCredentials, the authentication process requires
    to have an access to a separate event loop. The normal calls
    are run in a separate loop managed by the fsspec, but the
    DefaultCredentials assumes that the callee is managing their
    own event loop. This function checks whether is there any
    event loop set, and if not it creates a temporary event loop
    and set it. After the context is finalized, it restores the
    original event loop back (if there is any)."""

    try:
        original_loop = asyncio.get_event_loop()
        original_policy = asyncio.get_event_loop_policy()
    except RuntimeError:
        original_loop = None
        original_policy = None

    # From 3.8>= and onwards, asyncio changed the default
    # loop policy for windows to use proactor loops instead
    # of selector based ones. Due to that, proxied connections
    # doesn't work with the aiohttp and this is most likely an
    # upstream bug that needs to be solved outside of DVC. Until
    # such issue is resolved, we need to manage this;
    # https://github.com/aio-libs/aiohttp/issues/4536
    if sys.version_info >= (3, 8) and os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = original_loop or asyncio.new_event_loop()

    try:
        asyncio.set_event_loop(loop)
        yield
    finally:
        if original_loop is None:
            loop.close()
        asyncio.set_event_loop(original_loop)
        asyncio.set_event_loop_policy(original_policy)


class AzureAuthError(DvcException):
    pass


@memoize
def _az_config():
    # NOTE: ideally we would've used get_default_cli().config from
    # azure.cli.core, but azure-cli-core has a lot of conflicts with other
    # dependencies. So instead we are just use knack directly
    from knack.config import CLIConfig

    config_dir = os.getenv(
        "AZURE_CONFIG_DIR", os.path.expanduser(os.path.join("~", ".azure"))
    )
    return CLIConfig(config_dir=config_dir, config_env_var_prefix="AZURE")


# pylint:disable=abstract-method
class AzureFileSystem(ObjectFSWrapper):
    scheme = Schemes.AZURE
    PATH_CLS = CloudURLInfo
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))
    REQUIRES = {
        "adlfs": "adlfs",
        "knack": "knack",
        "azure-identity": "azure.identity",
    }

    @classmethod
    def _strip_protocol(cls, path: str):
        bucket = infer_storage_options(path).get("host")
        if bucket:
            return path

        bucket = _az_config().get("storage", "container_name", None)
        return f"azure://{bucket}"

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        ops = infer_storage_options(urlpath)
        if "host" in ops:
            return {"bucket": ops["host"]}
        return {}

    def _prepare_credentials(self, **config):
        from azure.identity.aio import DefaultAzureCredential

        # Disable spam from failed cred types for DefaultAzureCredential
        logging.getLogger("azure.identity.aio").setLevel(logging.ERROR)

        login_info = {}
        login_info["connection_string"] = config.get(
            "connection_string",
            _az_config().get("storage", "connection_string", None),
        )
        login_info["account_name"] = config.get(
            "account_name", _az_config().get("storage", "account", None)
        )
        login_info["account_key"] = config.get(
            "account_key", _az_config().get("storage", "key", None)
        )
        login_info["sas_token"] = config.get(
            "sas_token", _az_config().get("storage", "sas_token", None)
        )
        login_info["tenant_id"] = config.get("tenant_id")
        login_info["client_id"] = config.get("client_id")
        login_info["client_secret"] = config.get("client_secret")

        if not (login_info["account_name"] or login_info["connection_string"]):
            raise AzureAuthError(
                "Authentication to Azure Blob Storage requires either "
                "account_name or connection_string.\nLearn more about "
                "configuration settings at "
                + format_link("https://man.dvc.org/remote/modify")
            )

        any_secondary = any(
            value for key, value in login_info.items() if key != "account_name"
        )
        if (
            login_info["account_name"]
            and not any_secondary
            and not config.get("allow_anonymous_login")
        ):
            login_info["credential"] = DefaultAzureCredential(
                exclude_interactive_browser_credential=False
            )

        for login_method, required_keys in [  # noqa
            ("connection string", ["connection_string"]),
            (
                "AD service principal",
                ["tenant_id", "client_id", "client_secret"],
            ),
            ("account key", ["account_name", "account_key"]),
            ("SAS token", ["account_name", "sas_token"]),
            (
                f"default credentials ({_DEFAULT_CREDS_STEPS})",
                ["account_name", "credential"],
            ),
            ("anonymous login", ["account_name"]),
        ]:
            if all(login_info.get(key) is not None for key in required_keys):
                break
        else:
            login_method = None

        self.login_method = login_method
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from adlfs import AzureBlobFileSystem
        from azure.core.exceptions import AzureError

        try:
            with _temp_event_loop():
                file_system = AzureBlobFileSystem(**self.fs_args)
        except (ValueError, AzureError) as e:
            raise AzureAuthError(
                f"Authentication to Azure Blob Storage via {self.login_method}"
                " failed.\nLearn more about configuration settings at"
                f" {format_link('https://man.dvc.org/remote/modify')}"
            ) from e

        return file_system

    def open(
        self, path_info, mode="r", **kwargs
    ):  # pylint: disable=arguments-differ
        with _temp_event_loop():
            return self.fs.open(self._with_bucket(path_info), mode=mode)
