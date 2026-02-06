"""Deprecated shim.

This Docker-image script historically re-exported the stat latency data utilities.
"""

from analyzer.log_utils.data_utils import *  # noqa: F401,F403
