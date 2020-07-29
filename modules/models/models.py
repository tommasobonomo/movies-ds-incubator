from typing import Any, Dict, Union

import catboost
import pandas as pd


class CatBoostModel(catboost.CatBoostClassifier):
    """
    Catboost model optimizer with hyperparameter search based on Bayesian optimization.
    """

    def __init__(
        self,
        categorical_columns: Any = None,
        text_columns: Any = None,
        **catboost_param
    ):
        super().__init__(**catboost_param)
        self.categorical_columns = categorical_columns or catboost_param.get(
            "cat_features"
        )
        self.text_columns = text_columns or catboost_param.get("text_features")

    def fit(self, X: Union[pd.DataFrame, pd.Series], y: Any = None, **fit_params: Dict):
        train_pool = catboost.Pool(
            X, y, cat_features=self.categorical_columns, text_features=self.text_columns
        )

        return super().fit(train_pool, **fit_params)

    def predict(self, X: Union[pd.DataFrame, pd.Series], **predict_params: Dict):
        pool = catboost.Pool(
            X, cat_features=self.categorical_columns, text_features=self.text_columns
        )
        return super().predict(pool, **predict_params)

    def predict_proba(
        self, X: Union[pd.DataFrame, pd.Series], **predict_proba_params: Dict
    ):
        pool = catboost.Pool(
            X, cat_features=self.categorical_columns, text_features=self.text_columns
        )
        return super().predict_proba(pool, **predict_proba_params)

    def score(self, X: Union[pd.DataFrame, pd.Series], y: Any = None):
        pool = catboost.Pool(
            X, y, cat_features=self.categorical_columns, text_features=self.text_columns
        )
        return super().score(pool)

    def evaluate(
        self,
        X_test: Union[pd.DataFrame, pd.Series],
        y_test: Any,
        metric: str,
        **eval_params: Dict
    ):
        pool = catboost.Pool(
            X_test,
            y_test,
            cat_features=self.categorical_columns,
            text_features=self.text_columns,
        )
        return super().eval_metrics(pool, [metric], **eval_params)

    def cv(
        self,
        X: Union[pd.DataFrame, pd.Series],
        y: Any,
        verbose: bool = False,
        **cv_params: Dict
    ) -> Union[pd.DataFrame, Dict]:
        pool = catboost.Pool(
            X, y, cat_features=self.categorical_columns, text_features=self.text_columns
        )
        validation_scores = catboost.cv(pool, self.get_params(), **cv_params)
        if verbose:
            print(validation_scores)

        return validation_scores
