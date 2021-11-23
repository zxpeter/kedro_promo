#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

"""
@File    : pipeline.py
@Author  : Peter Zhao
@Time    : 11/24/21 12:00 AM
"""
from kedro.pipeline import Pipeline, node

from .nodes import raw_data_mocker


def create_pipeline() -> Pipeline:
    """Creates the pipeline for data mocker."""
    return Pipeline(
        [
            node(
                func=raw_data_mocker,
                inputs=[
                    "params:promo_data_fabricator",
                ],
                outputs="promo_mocked_data",
            )
        ]
    )