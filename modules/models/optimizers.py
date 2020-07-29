import logging
import time
from typing import Any, Dict, Optional, Tuple

import optuna
import pandas as pd
from sklearn.model_selection import (
    KFold,
    StratifiedKFold,
    cross_val_score,
    train_test_split,
)


class OptunaOptimizer:
    """
    Base model optimizer with hyper-parameter search based on Optuna.
    """

    def __init__(
        self,
        model: Any,
        data: Tuple[pd.DataFrame, pd.Series],
        n_fold: int = 3,
        early_stopping_rounds: int = 30,
        is_stratified: bool = True,
        is_shuffle: bool = True,
    ):
        self.model = model
        self.data = data
        self.n_fold = n_fold
        self.early_stopping_rounds = early_stopping_rounds
        self.is_stratified = is_stratified
        self.is_shuffle = is_shuffle

        if self.is_stratified:
            self.cross_validation_split = StratifiedKFold(
                n_splits=self.n_fold, shuffle=self.is_shuffle
            )
        else:
            self.cross_validation_split = KFold(
                n_splits=self.n_fold, shuffle=self.is_shuffle
            )

    @staticmethod
    def _construct_trial_grid(trial: optuna.Trial, param_space: Dict):
        param_grid = {}
        for name, params in param_space.items():
            param_type = params[0]
            if param_type == "categorical":
                choices = params[1]
                param_grid[name] = trial.suggest_categorical(name, choices)
            elif param_type == "discrete_uniform":
                low, high, q = params[1], params[2], params[3]
                param_grid[name] = trial.suggest_discrete_uniform(name, low, high, q)
            elif param_type == "loguniform":
                low, high = params[1], params[2]
                param_grid[name] = trial.suggest_loguniform(name, low, high)
            elif param_type == "uniform":
                low, high = params[1], params[2]
                param_grid[name] = trial.suggest_uniform(name, low, high)
            elif param_type == "float":
                low, high = params[1], params[2]
                step, log = None, False
                if len(params) > 3:
                    step = params[3]
                if len(params) > 4:
                    log = params[4]
                param_grid[name] = trial.suggest_float(
                    name, low, high, step=step, log=log
                )
            elif param_type == "int":
                low, high = params[1], params[2]
                step, log = 1, False
                if len(params) > 3:
                    step = params[3]
                if len(params) > 4:
                    log = params[4]
                param_grid[name] = trial.suggest_int(
                    name, low, high, step=step, log=log
                )
            else:
                raise ValueError(
                    f"Undefined sampling method given for trial object: {name}: {params}"
                )

        return param_grid

    def _objective(self, trial: optuna.Trial, param_space: Dict, scoring: str):
        param_grid = OptunaOptimizer._construct_trial_grid(trial, param_space)
        self.model.set_params(**param_grid)
        scores = cross_val_score(
            self.model,
            self.data[0],
            self.data[1],
            cv=self.cross_validation_split,
            scoring=scoring,
        )
        return scores.mean()

    def optimize(
        self,
        param_space: Dict,
        n_trials: Optional[int] = None,
        timeout: Optional[int] = None,
        direction: str = "maximize",
        scoring="accuracy",
        log_verbose: str = "INFO",
    ):

        if not n_trials and not timeout:
            raise ValueError("At least one of n_trials and timeout should be defined.")

        optuna.logging.set_verbosity(logging.getLevelName(log_verbose))
        study = optuna.create_study(direction=direction)
        try:
            study.optimize(
                lambda trial: self._objective(trial, param_space, scoring),
                n_trials=n_trials,
                timeout=timeout,
            )
        except KeyboardInterrupt:
            pass

        return study.best_params
