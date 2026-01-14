"""Legacy compatibility shim.

The active cleanup implementation lives in `conflux_deployer.resource_cleanup.manager`.
This module is kept to avoid breaking old import paths.
"""

from .manager import ResourceCleanupManager

__all__ = ["ResourceCleanupManager"]
