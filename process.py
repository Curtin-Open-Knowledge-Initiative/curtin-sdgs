# Reporting template
#
# Copyright 2020-21 ######
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors: COKI Team
import json
from pathlib import Path
import os

import pandas as pd
from google.cloud import bigquery
from pathlib import Path
import numpy as np
import plotly.graph_objects as go
from typing import Optional, Callable, Union

from observatory.reports import report_utils
from precipy.analytics_function import AnalyticsFunction
from report_data_processing.sql import (
    sdgs,
    filter_query,
    groupby_institution
)

from report_graphs import (
    SingleRadarPlot
)

# Replace with applicable project name
PROJECT_ID = 'coki-scratch-space'
TEMPDIR = Path('tempdir')


def generate_sdg_truth_table(af: AnalyticsFunction):
    """
    Create the SDG truth table for the full DOI table and save as new table in BigQuery
    """

    print("Generating the Truth Table")
    with bigquery.Client() as client:
        job_config = bigquery.QueryJobConfig(destination="coki-scratch-space.curtin.doi_sdgs",
                                             create_disposition="CREATE_IF_NEEDED",
                                             write_disposition="WRITE_TRUNCATE")

        # Start the query, passing in the extra configuration.
        query_job = client.query(sdgs, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

    print("...completed")


def filter_data(af: AnalyticsFunction):
    """
    Filter the truth table for DOIs of interest
    """

    print("Filtering the SDG Truth Table")
    with bigquery.Client() as client:
        job_config = bigquery.QueryJobConfig(destination="coki-scratch-space.curtin.filtered_doi_sdgs",
                                             create_disposition="CREATE_IF_NEEDED",
                                             write_disposition="WRITE_TRUNCATE")

        # Start the query, passing in the extra configuration.
        query_job = client.query(filter_query, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.
    print("...completed")


def group_and_download_data(af: AnalyticsFunction):
    """
    Download the filtered SDG data by year and institution
    """

    print("Downloading filtered data...")
    df = pd.read_gbq(groupby_institution,
                     project_id=PROJECT_ID)

    df.to_csv(TEMPDIR / 'outputs.csv')
    af.add_existing_file('outputs.csv')
    print("...completed")


def analyse(af: AnalyticsFunction):
    outputs = pd.read_csv(TEMPDIR / 'outputs.csv')
    sdg_cols = [col for col in outputs.columns if (col.startswith('sdg_') & ~(col.endswith('sustainab')))]
    sussdg_cols = [col for col in outputs.columns if (col.startswith('sdg_') & (col.endswith('sustainab')))]

    # Calculate Percentages by Year
    for column in sdg_cols:
        outputs[f'pc_{column}'] = outputs[column] / outputs.total_outputs * 100

    for column in sussdg_cols:
        outputs[f'sus_pc_{column}'] = outputs[column] / outputs.total_outputs * 100
    outputs.to_csv(TEMPDIR / 'summary_by_year.csv')
    af.add_existing_file(TEMPDIR / 'summary_by_year.csv')


def radar_plots(af: AnalyticsFunction):
    """
    Construct Radar Plots for Each University
    """

    summary = pd.read_csv(TEMPDIR / 'summary_by_year.csv')
    unis = summary.identifier.unique()
    thetas = [c for c in summary.columns if c.startswith('pc_sdg_')]
    for uni in unis:
        plot = SingleRadarPlot(df=summary,
                               thetas=thetas,
                               identifier=uni,
                               focus_year=2020)
        fig = plot.plotly()
        name = summary[summary.identifier==uni]['name'].values[0]
        fig.update_layout(title=name)
        fig.write_image(TEMPDIR / f'{name}.png')
        fig.write_html(TEMPDIR / f'{name}.html')


def load_cache_data(af: AnalyticsFunction,
                    function_name: Union[str, Callable],
                    filename: Optional[Union[str, Path]] = None,
                    element: Optional[str] = None):
    """Convenience function for loading previously prepared DataFrames from the cache

    :param filename:
    :param function_name:
    :param element: Component of the HDFS filecache to load
    :param af

    Downloaded query data is collected as DataFrames and stored in and HDFS store as DataFrames or
    as a csv file. This is a convenience function for reloading data from those dataframes.
    """

    if callable(function_name):
        afunction_name = function_name.__name__
    else:
        afunction_name = function_name

    filename = Path(filename)

    store_filepath = af.path_to_cached_file(
        filename, afunction_name)

    if filename.suffix == '.hd5':
        with pd.HDFStore(store_filepath) as store:
            if f"/{element}" not in store.keys():
                return None
            df = store[element]
    elif filename.suffix == '.csv':
        df = pd.read_csv(store_filepath)
    else:
        df = None

    return df
