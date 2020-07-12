import os
from typing import List

import pandas as pd
import typer
from bs4 import BeautifulSoup
from requests import get

app = typer.Typer()


def extract_table_from_page(raw_html: str) -> pd.DataFrame:
    """Extracts a single table from a given html page. Assumes there is only one table in the specfied webpage.

    Args:
        raw_html (str): A string containing the html of the webpage

    Returns:
        pd.DataFrame: DataFrame that contains the table specified in the HTML
    """

    soup = BeautifulSoup(raw_html, "html.parser")
    table_html = soup.table

    tables = pd.read_html(table_html.prettify(), flavor="html5lib")
    assert len(tables) == 1, "There is more than one table in the HTML"
    return tables[0]


def scrape_numbers_website(verbose: bool = False) -> pd.DataFrame:
    """Scrapes the tables on the website www.the-numbers.com. We get the movie, the release date, and information on budget and box office revenue.

    Args:
        verbose (bool, optional): A flag to print current state of the scraping. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with title, release date, revenue and budget for different movies
    """
    base_url = "https://www.the-numbers.com/movie/budgets/all"
    indicator = 1

    all_tables: List[pd.DataFrame] = []
    end = False
    while not end:
        if verbose:
            typer.echo(f"Scraping page {indicator}-{indicator+100}")

        # Make request and extract table
        res = get(f"{base_url}/{indicator}")
        temp_table = extract_table_from_page(str(res.content))

        if temp_table.empty:
            end = True
        else:
            all_tables.append(temp_table)
            indicator += 100

    if verbose:
        typer.echo("Finished scraping")

    final_table = pd.concat(all_tables).drop(columns="Unnamed: 0").convert_dtypes()
    final_table = final_table.reset_index(drop=True)
    final_table.columns = [" ".join(column.split()) for column in final_table.columns]

    for column in final_table.columns:
        if column not in ["Release Date", "Movie"]:
            numeric_column = pd.to_numeric(
                final_table[column].str.replace("$", "").str.replace(",", "")
            )
            numeric_column.name = final_table[column].name + " (USD)"
            final_table = final_table.drop(columns=column)
            final_table = pd.concat([final_table, numeric_column], axis=1)

    return final_table


@app.command()
def scrape(
    output_name: str = "numbers_data.csv",
    output_dir_path: str = ".",
    verbose: bool = False,
) -> None:
    typer.echo("Scraping the-numbers.com... \U0001f528")
    scraped_table = scrape_numbers_website(verbose=verbose)

    typer.echo(
        f"Saving final csv to {os.path.join(output_dir_path, output_name)}... \U0001f4be"
    )
    scraped_table.to_csv(
        os.path.join(output_dir_path, output_name), index=False, header=True
    )

    typer.echo("Done! \U00002714")


if __name__ == "__main__":
    app()
