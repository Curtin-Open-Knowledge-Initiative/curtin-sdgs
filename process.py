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
import jinja2
import plotly.express as px

from observatory.reports import report_utils
from precipy.analytics_function import AnalyticsFunction
from report_data_processing.sql import load_sql_to_string

from report_graphs import (
    SingleRadarPlot
)

from parameters import *


def generate_sdg_truth_table(af: AnalyticsFunction,
                             rerun: bool=RERUN,
                             verbose: bool=VERBOSE):
    """
    Create the SDG truth table for the full DOI table and save as new table in BigQuery
    """

    print("Generating the Truth Table")
    query = load_sql_to_string('sdgs.sql',
                               dict(table=DOI_TABLE),
                               directory=SQL_DIRECTORY)
    if not report_utils.bigquery_rerun(af, rerun, verbose):
        print(f"""Query is:

    {query}

    """)
        return

    with bigquery.Client() as client:
        job_config = bigquery.QueryJobConfig(destination=TRUTH_TABLE,
                                             create_disposition="CREATE_IF_NEEDED",
                                             write_disposition="WRITE_TRUNCATE")

        # Start the query, passing in the extra configuration.
        query_job = client.query(query, job_config=job_config, project=PROJECT_ID)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

    print("...completed")


def filter_data(af: AnalyticsFunction,
                rerun: bool=RERUN,
                verbose: bool=VERBOSE):
    """
    Filter the truth table for DOIs of interest
    """

    print("Filtering the SDG Truth Table")
    query = load_sql_to_string('filter_query.sql',
                               dict(doi_table=DOI_TABLE,
                                    truth_table=TRUTH_TABLE),
                               directory=SQL_DIRECTORY)

    if not report_utils.bigquery_rerun(af, rerun, verbose):
        print(f"""Query is:

        {query}

        """)
        return

    with bigquery.Client() as client:
        job_config = bigquery.QueryJobConfig(destination=JOIN_TABLE,
                                             create_disposition="CREATE_IF_NEEDED",
                                             write_disposition="WRITE_TRUNCATE")

        # Start the query, passing in the extra configuration.
        query_job = client.query(query, job_config=job_config, project=PROJECT_ID)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

    print("...completed")


def group_and_download_data(af: AnalyticsFunction,
                            rerun: bool = RERUN,
                            verbose: bool = VERBOSE):
    """
    Download the filtered SDG data by year and institution
    """

    query_template = load_sql_to_string('groupby_institution.sql.jinja2',
                                        directory=SQL_DIRECTORY)

    data = dict(
        table=JOIN_TABLE,
        sdgs=SDG_LIST
    )

    query = jinja2.Template(query_template).render(data)
    if not report_utils.bigquery_rerun(af, rerun, verbose):
        print(f"""Query is:
    
    {query}
    
    """)
        return

    data = pd.read_gbq(query=query,
                       project_id=PROJECT_ID)
    data.to_csv(DATA_FOLDER / 'institutions.csv')
    af.add_existing_file(DATA_FOLDER / 'institutions.csv')

    if verbose:
        print('...completed')


def analyse(af: AnalyticsFunction):
    outputs = pd.read_csv(DATA_FOLDER / 'institutions.csv')
    sdg_cols = [col for col in outputs.columns if (col.startswith('sdg_') & ~(col.endswith('sustainab')))]
    sussdg_cols = [col for col in outputs.columns if (col.startswith('sdg_') & (col.endswith('sustainab')))]

    # Calculate Percentages by Year
    for column in sdg_cols:
        outputs[f'pc_{column}'] = outputs[column] / outputs.total_outputs * 100

    for column in sussdg_cols:
        outputs[f'sus_pc_{column}'] = outputs[column] / outputs.total_outputs * 100
    outputs.to_csv(DATA_FOLDER / 'summary_by_year.csv')
    af.add_existing_file(DATA_FOLDER / 'summary_by_year.csv')


def radar_plots(af: AnalyticsFunction):
    """
    Construct Radar Plots for Each University
    """

    summary = pd.read_csv(DATA_FOLDER / 'summary_by_year.csv')
    unis = summary.identifier.unique()
    thetas = [c for c in summary.columns if c.startswith('pc_sdg_')]
    for uni in unis:
        plot = SingleRadarPlot(df=summary,
                               thetas=thetas,
                               identifier=uni,
                               focus_year=2020)
        fig = plot.plotly()
        name = summary[summary.identifier == uni]['name'].values[0]
        fig.update_layout(title=name)
        fig.write_image(TEMPDIR / f'{name}.png')
        fig.write_html(TEMPDIR / f'{name}.html')


def heatmap(af: AnalyticsFunction):
    """
    Generate a heatmap for Curtin University SDG cross correlation
    """

    data = pd.read_csv(DATA_FOLDER/ 'summary_by_year.csv')
    curtin = data[data.identifier=='https://ror.org/02n415q13']

    sdg_cols = [c for c in curtin.columns if c.endswith('_crosscorr')]
    id_cols = ['identifier', 'name', 'published_year']

    sums = curtin[id_cols + sdg_cols + ['total_outputs']].groupby(id_cols).sum()
    matrix = []
    for sdg1 in SDG_LIST:
        matrix.append([curtin[f'{sdg1}_{sdg2}_crosscorr'].iloc[0] / curtin[sdg1].iloc[0] * 100
                       for sdg2 in SDG_LIST])

    fig = px.imshow(matrix, color_continuous_scale="PuRd",
                    x=SDG_LIST,
                    y=SDG_LIST)
    fig.show()
    fig.write_html("cross_correlation.html")


def boxplots(af: AnalyticsFunction):
    """
    Boxplots to compare levels of SDG by country/region
    """

    data = pd.read_csv(DATA_FOLDER / 'institutions.csv')
    id_cols = ['identifier', 'name', 'country', 'region']
    summed = data.groupby(id_cols).sum()
    summed['pc_total_sdgs'] = summed.total_sdgs / summed.total_outputs * 100

    grouped = summed.groupby('country').median()
    ordered = grouped.sort_values('pc_total_sdgs')
    order = ordered.index.values
    summed.reset_index(inplace=True)
    summed['country'] = pd.Categorical(
        summed.country,
        categories=order,
        ordered=True
    )
    summed.sort_values('country', inplace=True)

    fig = px.box(summed,
                 x='country',
                 y='pc_total_sdgs',
                 points='all',
                 hover_data=['name', 'pc_total_sdgs', 'total_outputs'])
    fig.show()
    fig.write_html('median_by_country.html')
    pass


