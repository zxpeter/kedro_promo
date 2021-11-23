#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

"""
@File    : nodes.py
@Author  : Peter Zhao
@Time    : 11/24/21 12:00 AM
"""


from typing import Any, Callable, Dict, Union

import logging
import pyspark
import pandas as pd
from data_fabricator.v0.core.fabricator import MockDataGenerator

_IGNORE_DATAFRAMES_WITH_PREFIX = ["_TEMP_", "_CATALOG_"]


def raw_data_mocker(
    fabrication_params: Dict[str, Any],
    **source_dfs: Dict[str, Union[pd.DataFrame, pyspark.sql.DataFrame]],
) -> Dict[str, pd.DataFrame]:

    """Fabricates datasets.

    This node passes configuration to ``MockDataGenerator`` data fabricator to fabricate
    datasets.

    Args:
        fabrication_params: Fabrication parameters to pass to ``MockDataGenerator``.
        source_dfs: Optional real-world dataframes to add to ``MockDataGenerator``.

    Returns:
        A dictionary with the fabricated pandas dataframes.
    """

    mock_generator = MockDataGenerator(instructions=fabrication_params)

    # #  comment out for testing period
    # if source_dfs:  # for real client data injection
    #     for df_name, df in source_dfs.items():
    #         if isinstance(df, pd.DataFrame):
    #             mock_generator.all_dataframes[df_name] = df
    #         elif isinstance(df, pyspark.sql.DataFrame):
    #             mock_generator.all_dataframes[df_name] = df.toPandas()

    mock_generator.generate_all()

    output = {
        name: mock_generator.all_dataframes[name]
        for name in fabrication_params
        if not any(name.startswith(prefix) for prefix in _IGNORE_DATAFRAMES_WITH_PREFIX)
    }
    logging.info(output)

    return output
