from typing import Dict, List, Tuple, Union, Optional

import optuna
import pandas as pd
import typer
from sklearn.compose import make_column_transformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline

app = typer.Typer()


def data_preprocessing(
    data: pd.DataFrame, feature_columns: List[str],
) -> Tuple[pd.DataFrame, pd.Series]:
    """Preprocess the `feature_columns` in data to a matrix of features `X` and an array of target variables `y`.

    Args:
        data (pd.DataFrame): The original data.
        feature_columns (List[str]): The columns to keep. Will discard any rows that contain a NaN value in any of the feature columns or target column.
        target_column (str, optional): The target column that must be predicted. Defaults to "movie_classification".

    Returns:
        Tuple[pd.DataFrame, pd.Series]: The extracted tuple of matrix of features `X` and target array `y`.
    """
    classified_data = data[data["movie_classification"] != "unclassified"]
    classified_data = classified_data[
        feature_columns + ["movie_classification"]
    ].dropna()

    X = classified_data[feature_columns]
    y = classified_data["movie_classification"]
    return X, y


def random_forest_optimization(
    trial: optuna.Trial, data: Tuple[pd.DataFrame, pd.Series], text_columns: List[str],
):
    """Optimization function that finds hyperparameters through the Optuna framework

    Args:
        trial (optuna.Trial): The trial in the optuna.Study
        data (Tuple[pd.DataFrame, pd.Series]): Data on which to perform optimization. Should not be testing data.
        text_columns (List[str]): Columns of the data that are textual.
    """
    X_train, y_train = data
    param_grid = {
        "n_estimators": trial.suggest_int("n_estimators", 50, 900, step=10),
        "max_features": trial.suggest_categorical(
            "max_features", ["log2", "sqrt", None]
        ),
        "max_depth": trial.suggest_int("max_depth", 2, 100),
    }
    pipe = model_pipeline(text_columns, param_grid)

    cross_validation_split = StratifiedKFold(n_splits=3, shuffle=True)
    scores = cross_val_score(
        pipe, X_train, y_train, cv=cross_validation_split, scoring="accuracy"
    )

    return scores.mean()


def model_pipeline(
    text_columns: List[str], param_grid: Dict[str, Union[str, float, int]] = {}
) -> Pipeline:
    """Creates a pipeline with conversion of text into numerical columns and a random forest classifier as estimator.

    Args:
        text_columns (List[str]): The list of columns that contain textual information, will be converted using tf-idf.

    Returns:
        Pipeline: The final pipeline that can be used with original data.
    """
    transformers = [(TfidfVectorizer(), col) for col in text_columns]
    tf_idf = make_column_transformer(*transformers, remainder="passthrough")
    pipeline = Pipeline(
        [("tf-idf", tf_idf), ("random_forest", RandomForestClassifier(**param_grid))]
    )
    return pipeline


@app.command()
def evaluate(data_path: str):
    """Evaluates a default Random Forest model on the dataset encoded with tf-idf

    Args:

        data_path (str): Path to the data file
    """

    typer.echo("Loading data...")
    data = pd.read_csv(data_path).convert_dtypes()
    feature_columns = [
        "description",
        "production_countries",
        "language",
        "director",
        "production_companies",
        "cast",
        "description2",
        "tagline",
    ]
    X, y = data_preprocessing(data, feature_columns)
    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y)

    typer.echo("Training model...")
    pipe = model_pipeline(feature_columns)
    pipe.fit(X_train, y_train)
    accuracy = pipe.score(X_test, y_test)

    typer.echo(f"Finished training model. Accuracy: {100 * accuracy:.2f}%")


@app.command()
def optimize(
    data_path: str, n_trials: Optional[int] = None, timeout: Optional[int] = None
):
    """Optimises the Random Forest model with the Optuna framework, on data encoded through tf-idf

    Args:

        data_path (str): Path to the data file

        n_trials (Optional[int]): Number of trials to execute

        timeout (Optional[int]): Maximum time to wait for optimization in seconds

    Raises:

        ValueError: At least one of n_trials and timeout should be defined.
    """

    if not n_trials and not timeout:
        raise ValueError("At least one of n_trials and timeout should be defined.")

    typer.echo("Loading data...")
    data = pd.read_csv(data_path).convert_dtypes()
    feature_columns = [
        "description",
        "production_countries",
        "language",
        "director",
        "production_companies",
        "cast",
        "description2",
        "tagline",
    ]
    X, y = data_preprocessing(data, feature_columns)
    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y)

    typer.echo("Creating Optuna study and running it...")

    study = optuna.create_study(direction="maximize")
    study.optimize(
        lambda trial: random_forest_optimization(
            trial, (X_train, y_train), feature_columns
        ),
        n_trials=n_trials,
        timeout=timeout,
    )

    typer.echo(f"Best accuracy: {study.best_value}")
    typer.echo(f"With params: {study.best_params}")


if __name__ == "__main__":
    app()
