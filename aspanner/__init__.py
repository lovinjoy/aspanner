#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Easy Async Spanner, wrapped python-spanner.gapic, async call rpc directly.
"""
__author__ = 'Zagfai'
__date__ = '2022-06'


__version__ = '0.6.1'

from google.cloud.spanner_v1 import param_types
from google.cloud.spanner_v1.keyset import KeyRange
from google.cloud.spanner_v1.keyset import KeySet

from .client import Aspanner

COMMIT_TIMESTAMP = "spanner.commit_timestamp()"
"""Placeholder be used to store commit timestamp of a transaction in a column.
This value can only be used for timestamp columns that have set the option
``(allow_commit_timestamp=true)`` in the schema.
"""

__all__ = (
        "__version__",
        "Aspanner",
        "KeySet",
        "KeyRange",
        "param_types",
        "COMMIT_TIMESTAMP",
)
