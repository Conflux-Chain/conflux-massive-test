"""Legacy compatibility shim.

The active image implementation lives in `conflux_deployer.image_management.manager`.
This module is kept to avoid breaking old import paths.
"""

from .manager import ImageManager, generate_user_data_script

__all__ = ["ImageManager", "generate_user_data_script"]
