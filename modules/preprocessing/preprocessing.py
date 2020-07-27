from typing import Any, Dict, List, Optional
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted


class BaselinePreprocessor(BaseEstimator, TransformerMixin):
    """
    Base processor for movies classification
    """
    def __init__(self, drop_cols: Optional[List[str]] = None,
                 na_cols: Optional[List[str]] = None,
                 target_col: str = 'movie_classification',
                 merge_descr: bool = True, drop_na_rows: bool = True,
                 drop_by_target: bool = True):
        """
        Init method for processor.
        :param drop_cols: list of column names will be dropped. No dropping if None - Optional[List[str]]
        :param na_cols: list of column names filled na values with empty string.
        Columns need to be string type (object or stringdtype). No filling if None - Optional[List[str]]
        :param target_col: target column for movie classification. will use if drop_by_target is True
        and target series selection- str
        :param merge_descr: merge two description columns into one if set - bool
        :param drop_na_rows: drop rows with any na column if set - bool
        :param drop_by_target: drop rows with target label 'unclassified' if set - bool
        :return: self: the class object - an instance of the transformer - Transformer
        """
        # if the columns parameter is not a list, make it into a list
        if drop_cols is None:
            drop_cols = []

        if na_cols is None:
            na_cols = []

        self.drop_cols_ = drop_cols
        self.na_cols_ = na_cols
        self.target_col_ = target_col
        self.merge_descr_ = merge_descr
        self.drop_na_rows_ = drop_na_rows
        self.drop_by_target_ = drop_by_target

    def fit(self, X: pd.DataFrame, y=None):
        """
        Method to check columns and setup column conf etc.
        :param X: dataframe will be transformed - Dataframe
        :param y: target vector - Series
        :return: self: the class object - an instance of the transformer - Transformer
        """
        self._check_column_names(X)
        self.feature_names_ = [col for col in X.columns if col not in self.drop_cols_]
        self._check_column_length()
        return self

    def transform(self, X: pd.DataFrame) -> (pd.DataFrame, pd.Series):
        """Returns a pandas DataFrame after transformation mentioned in init function.

        :param X: ``pd.DataFrame`` on which we apply the transformation
        :returns: ``pd.DataFrame`` and ``pd.Series`` for training dataframe and target series
        """
        check_is_fitted(self)
        X = X.copy()

        # first drop redundant columns
        if self.drop_cols_:
            X = X.drop(columns=self.drop_cols_)

        # second fill na values with empty string for given columns
        if self.na_cols_:
            X = self._fill_na(X)

        # third merge description columns if needed
        check_col_exist_ = (col in X.columns for col in ['description', 'description2'])
        if self.merge_descr_ and all(check_col_exist_):
            X["description"] = X.apply(
                lambda x: x["description"] if str(x["description"]) == str(x["description2"])
                else str(x["description"]) + " " + str(x["description2"]), axis=1)
            X["description"] = X["description"].str.strip()
            X = X.drop(columns="description2")

        # forth drop any row with na values after filling if needed
        if self.drop_na_rows_:
            X = X.dropna(how='any', axis=0)

        # fifth filter remaining rows by target col of needed
        if self.drop_by_target_:
            X = X[X[self.target_col_] != 'unclassified']

        # sixth drop target column and select y using target col
        y = X[self.target_col_]
        X = X.drop(columns=self.target_col_)

        return X, y

    def fit_transform(self, X: pd.DataFrame, y=None, **kwargs: Dict) -> (pd.DataFrame, pd.Series):
        """
        perform fit and transform over the data
        :param X: dataframe will be transformed - Dataframe
        :param y: target vector - Series
        :param kwargs: free parameters - dictionary
        :returns: ``pd.DataFrame`` and ``pd.Series`` for training dataframe and target series
        """
        self_ = self.fit(X, y)
        return self_.transform(X)

    @staticmethod
    def update_features(X: pd.DataFrame, col_list: List[str]) -> List[str]:
        return [col for col in col_list if col in X.columns]

    def _fill_na(self, X: pd.DataFrame) -> pd.DataFrame:
        """Fill na rows for given columns with empty string in the input DataFrame"""
        # check whether filling columns in text format
        text_cols = X.select_dtypes(include=['object', 'string'])

        for feature in self.na_cols_:
            if feature not in text_cols:
                raise KeyError(f"{feature} is given but the type of column is not one of the string dtype")

            X[feature] = X[feature].fillna('')

        return X

    def _check_column_names(self, X: pd.DataFrame):
        """Check if one or more of the columns provided doesn't exist in the input DataFrame"""
        non_existent_columns = set(self.drop_cols_).difference(X.columns)
        na_existence_columns = set(self.na_cols_).difference(X.columns)

        if self.drop_by_target_ and self.target_col_ not in X.columns:
            raise KeyError(f"{self.target_col_} is given as target column but not in DataFrame")

        if len(non_existent_columns) > 0:
            raise KeyError(f"{list(non_existent_columns)} column(s) not in DataFrame")

        if len(na_existence_columns) > 0:
            raise KeyError(f"{list(na_existence_columns)} column(s) not in DataFrame")

    def _check_column_length(self):
        """Check if all columns are dropped"""
        if len(self.feature_names_) == 0:
            raise ValueError(
                f"Dropping {self.drop_cols_} would result in an empty output DataFrame"
            )


class ToWideTransformer(BaseEstimator, TransformerMixin):
    """
    Returns a dataframe where values in column are not delimiter-separated lists
    anymore but one per row, with all other information duplicated on multiple rows
    """
    def __init__(self, column: str, delimiter: str = ",") -> None:
        """
        Constructor for the transformer.
        :param column: column name will be used for transformation - str
        :param delimiter: delimiter used for column value splitting - str
        """
        super(ToWideTransformer, self).__init__()
        self.column = column
        self.delimiter = delimiter

    def fit(self, X: pd.DataFrame, y=None, **kwargs: Dict):
        """
        Method to check columns.
        :param X: dataframe will be transformed - Dataframe
        :param y: target vector - Series
        :param kwargs: free parameters - dictionary
        :return: self: the class object - an instance of the transformer - Transformer
        """
        assert self.column in X.columns, "Column must be in dataframe"
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform tight format to wide format for a column.
        :param X: dataframe will be transformed - Dataframe
        :return: X: the transformed data - Dataframe
        """
        # Expand Series of column to DataFrame and add index as id for merging afterwards
        splitted_columns = X[self.column].str.split(self.delimiter, expand=True).reset_index()

        # Convert from wide format to long format and drop NAs
        long_format = (
            pd.melt(
                splitted_columns,
                id_vars="index",
                value_vars=splitted_columns.drop(columns="index").columns,
                value_name=self.column
            )
                .drop(columns="variable")
                .dropna()
        )

        # Strip all values
        long_format[self.column] = long_format[self.column].str.strip()

        # Merge dataframe
        merged_df = pd.merge(
            long_format,
            X.drop(columns=self.column).reset_index(),
            on="index",
            how="inner"
        )
        return merged_df.drop(columns="index")

    def fit_transform(self, X: pd.DataFrame, y=None, **kwargs: Dict) -> pd.DataFrame:
        """
        perform fit and transform over the data
        :param X: dataframe will be transformed - Dataframe
        :param y: target vector - Series
        :param kwargs: free parameters - dictionary
        :return: X: the transformed data - Dataframe
        """
        self_ = self.fit(X, y)
        return self_.transform(X)