"""Legacy compatibility shim.

This repository previously had a separate test controller implementation in
this module. The active implementation lives in `conflux_deployer.test_control.controller`.

Keeping this file avoids breaking old import paths while staying type-check clean.
"""

from .controller import TestController, TestResult

__all__ = ["TestController", "TestResult"]
