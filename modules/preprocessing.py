import pandas as pd


# Data filling and merging operations
def check_identical_df(df: pd.DataFrame, group_cols: list) -> bool:
    """Checks whether a given dataframe contains identical rows"""
    return df.groupby(group_cols).nunique(dropna=False).eq(1).all()


def create_target_feature(df: pd.DataFrame):
    """ Create target feature named movie_classification"""

    def target_classification(revenue, budget) -> str:
        if revenue == 0 or budget == 0:
            return "unclassified"

        if revenue >= 4.5 * budget:
            return "super hit"
        if 4.5 * budget >= revenue >= 2.5 * budget:
            return "blockbuster"
        if 2.5 * budget >= revenue >= budget:
            return "minor success"
        if budget >= revenue >= 1 / 3 * budget:
            return "flop"
        if 1 / 3 * budget >= revenue:
            return "box office bomb"

    df["movie_classification"] = df.apply(
        lambda row: target_classification(row["revenue"], row["budget"]), axis=1
    )


def print_stats(df, budget_col="budget", revenue_col="revenue"):
    """ Print several statistics and nan values of given dataset"""
    len_df = len(df)
    print(
        f"Lenght: {len_df} Available budget: {len_df - len(df[df[budget_col] == 0])}"
        f" Available revenue: {len_df - len(df[df[revenue_col] == 0])}"
    )
    print(f"Missing values (NaN): \n\n {df.isnull().sum()}")
    if "movie_classification" in df.columns:
        print(
            f'\nUsable rows according to target: {len_df - len(df[df.movie_classification == "unclassified"])}'
        )


def list_column_to_long_format(
    dataframe: pd.DataFrame, column: str, delimiter: str = ","
) -> pd.DataFrame:
    """Returns a dataframe where values in column are not delimiter-separated lists 
    anymore but one per row, with all other information duplicated on multiple rows"""
    assert column in dataframe.columns, "Column must be in dataframe"

    # Expand Series of column to DataFrame and add index as id for merging afterwards
    splitted_columns = dataframe[column].str.split(delimiter, expand=True).reset_index()

    # Convert from wide format to long format and drop NAs
    long_format = (
        pd.melt(
            splitted_columns,
            id_vars="index",
            value_vars=splitted_columns.drop(columns="index").columns,
            value_name=column,
        )
        .drop(columns="variable")
        .dropna()
    )

    # Strip all values
    long_format[column] = long_format[column].str.strip()

    # Merge dataframe
    merged_df = pd.merge(
        long_format,
        dataframe.drop(columns=column).reset_index(),
        on="index",
        how="inner",
    )
    return merged_df.drop(columns="index")
