from typing import Union, Any

import pandas as pd
import sweetviz
from pandas_profiling import ProfileReport


class EDA:
    def __init__(self, data: pd.DataFrame) -> None:
        """
        Constructor for the eda class.
        :param data: Dataframe will be used for EDA - Dataframe
        """
        self.data = data

    def print_stats(self, budget_col: str = 'budget', revenue_col: str = 'revenue') -> None:
        """
        Print several statistics and nan values of given dataset
        :param budget_col: Name of the column indicating movie budget - str
        :param revenue_col: Name of the column indicating movie revenue - str
        """
        len_df = len(self.data)
        zero_budget = len(self.data[self.data[budget_col] == 0])
        zero_revenue = len(self.data[self.data[revenue_col] == 0])

        print(f'Lenght: {len_df} Available budget: {len_df - zero_budget}'
              f' Available revenue: {len_df - zero_revenue}')

        print(f'Missing values (NaN): \n\n {self.data.isnull().sum()}')
        if 'movie_classification' in self.data.columns:
            unclassified_movies = len(self.data[self.data.movie_classification == "unclassified"])
            print(f'\nUsable rows according to target: {len_df - unclassified_movies}')

    def sweetviz_compare(self, test_data: pd.DataFrame, target_col: str) -> None:
        """
        Compare two dataframes and output as HTML file using sweetviz.
        https://github.com/fbdesignpro/sweetviz
        :param test_data: The dataset will be compared with original dataset - Dataframe
        :param target_col: The name of the target column - str
        :return: None
        """
        my_report = sweetviz.compare([self.data, "Data"], [test_data, "Test"], target_col)
        my_report.show_html("Report.html")  # Not providing a filename will default to SWEETVIZ_REPORT.html

    def sweetviz_analyze(self, target_col: str) -> None:
        """
        Analyze original dataframe and output as HTML file using sweetviz.
        https://github.com/fbdesignpro/sweetviz
        :param target_col: The name of the target column - str
        :return: None
        """
        my_report = sweetviz.analyze([self.data, "Data"], target_col)
        my_report.show_html("Report.html")  # Not providing a filename will default to SWEETVIZ_REPORT.html

    def pandas_profiling_analyze(self, title: str = 'Movie dataset', sort: Union[str, None] = None,
                                 explorative: bool = True, **kwargs: Any):
        """
        Generate pandas profiling report using pandas-profiling.
        https://pandas-profiling.github.io/
        :param title: The name of the report - str
        :param sort: Sort the variables asc(ending), desc(ending) or None (leaves original sorting). - Union[str, None]
        :param explorative: Whether use explorative config file or not. - bool
        :return: None
        """
        # Generate the Profiling Report
        profile = ProfileReport(self.data, title=title, html={'style': {'full_width': True}},
                                sort=sort, explorative=explorative, **kwargs)
        return profile.to_widgets()