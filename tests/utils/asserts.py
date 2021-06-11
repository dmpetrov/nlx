from typing import Any
from unittest.mock import ANY, Mock


def called_once_with_subset(m: Mock, *args: Any, **kwargs: Any) -> bool:
    m.assert_called_once()
    m_args, m_kwargs = m.call_args

    expected_args = m_args + (ANY,) * (len(m_args) - len(args))
    expected_kwargs = {k: kwargs.get(k, ANY) for k in m_kwargs}
    m.assert_called_with(*expected_args, **expected_kwargs)

    return True
