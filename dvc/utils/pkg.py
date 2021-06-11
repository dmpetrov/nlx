try:
    # file is created during dvc build
    from .build import PKG  # noqa, pylint:disable=unused-import
except ImportError:
    PKG = None
