# Copyright 2021 Curtin University
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

# Author: Cameron Neylon

import plotly.graph_objects as go
import pandas as pd

from typing import Optional, Union, List
from observatory.reports.abstract_chart import AbstractObservatoryChart


class SingleRadarPlot(AbstractObservatoryChart):
    """Generates a Plotly Radar chart
    """

    def __init__(self,
                 df: pd.DataFrame,
                 thetas: Union[str, list],
                 values: Optional[Union[str, list]] = None,
                 identifier: Optional[str] = None,
                 focus_year: Optional[int] = None,
                 theta_labels: Optional[Union[List[str], dict]] = None):
        """Initialisation function
        """

        self.df = df
        self.thetas = thetas
        self.values = values
        self.identifier = identifier
        self.focus_year = focus_year
        self.theta_labels = theta_labels
        self.figdata = None

    def process_data(self, **kwargs):
        figdata = self.df
        if self.identifier:
            figdata = figdata[figdata.identifier == self.identifier]
        if self.focus_year:
            year_column_name = list({'published_year', 'time_period'} & {*figdata.columns})[0]
            figdata = figdata[figdata[year_column_name] == self.focus_year]

        if not self.values:
            ## TODO Allow for multiple traces, eg by year?
            assert len(figdata) == 1

        theta_labels = self.thetas
        if self.theta_labels:
            if type(self.theta_labels) == dict:
                theta_labels = {self.theta_labels[th] for th in self.thetas}
            elif type(self.theta_labels) == list:
                assert len(self.thetas) == len(self.theta_labels)
                theta_labels = self.theta_labels

        values = [figdata[theta].values[0] for theta in self.thetas]

        fill = kwargs.get('fill')
        if not fill:
            fill = 'toself'
        self.figdata = go.Scatterpolar(
            r=values,
            theta=theta_labels,
            fill=fill)

    def plotly(self, **kwargs):
        if not self.figdata:
            self.process_data()

        fig = go.Figure(data=self.figdata)
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 10]
                ),
            ),
            showlegend=False
        )
        return fig
