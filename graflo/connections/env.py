"""The ``conn_proxy`` → environment-variable naming rule.

Every config resolved from the environment for a ``conn_proxy`` label reads
``{PREFIX}<FIELD>``, where *PREFIX* comes from :func:`proxy_env_prefix` unless
the caller passes one explicitly.
"""

from __future__ import annotations


def proxy_env_prefix(conn_proxy: str) -> str:
    """The environment-variable prefix of a ``conn_proxy`` label.

    ``user_service`` → ``USER_SERVICE_``; ``billing-api`` → ``BILLING_API_``.
    """
    return conn_proxy.upper().replace("-", "_") + "_"
