import os
from abc import ABC, abstractmethod
from configparser import ConfigParser
from collections import defaultdict


import numpy as np
import pandas as pd
import tmdbsimple as tmdb
import requests
import logging
import tqdm

import copyreg
import types
from multiprocessing import Pool, cpu_count, current_process
from multiprocessing_logging import install_mp_handler


def _pickle_method(m):
    """
    Boilerplate method for multiprocessing in instance class
    check here: https://stackoverflow.com/questions/25156768/cant-pickle-type-instancemethod-using-pythons-multiprocessing-pool-apply-a
    """
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copyreg.pickle(types.MethodType, _pickle_method)


class DataFillerAbstract(ABC):
    @abstractmethod
    def __init__(self, df: pd.DataFrame, fill_columns: dict, config_path: str = 'modules/config.ini',
                 log_path: str = 'log.txt', log_level: int = logging.INFO):
        """
        Init method for data filler. It initializes API object, store a deep copy of dataframe,
        and setup logger etc.

        Args:
            df: The dataframe will be filled.
            fill_columns: The dict includes the columns will be filled.
                          Format: {column name: (original column name, na_val), ...}
                          column name is the name of the column will be filled in dataframe.
                          original column name is the name of the API field related to that column.
                          na_val is the marker value needs to be filled. For example, if 0 given, all rows
                          with 0 for that column will be filled by ino retrieved from API.
            config_path:  The config path needed for the class. It includes the related API auth keys.
            log_path:  The log file path.
            log_level: The log level the logger will be logging.

        Returns:
            None.

        Raises:
            AssertionError: If the given config path is not valid.
            ValueError: If a fill_column is not a column of given dataframe.
        """
        pass

    @abstractmethod
    def fill(self, n_workers: int = cpu_count()) -> pd.DataFrame:
        """
        The method fills the dataframe given during initialization of the class. It first
        filters the rows needs to be filled by checking given fill_columns argument. After filtering rows,
        It split these rows into n_workers chunk and proceed (fill the missing fields of rows if available on API)
        in parallel. After processing all chunks, all of them have merged into single dataframe, and original dataframe
        has been updated by this new dataframe. Finally, the dtypes conversion is done for ensuring that original dtypes
        are preserved.

        Args:
            n_workers: The number of workers for parallel processing.

        Returns:
            - : The filled dataframe.

        Raises:
            None.
        """
        pass

    @staticmethod
    def _get_logger(log_path: str, log_level: int) -> logging.Logger:
        """
        The static method setup logger. It setup an application logger called movies_ds.
        It creates two handler one for file handler to log on text file and other one is a stream handler
        log to stdout. The given log level is setup for both handler. At the end of the setup, multiprocessing
        handler is installed for pretty log from multiprocess.

        Args:
            log_path:  The log file path.
            log_level: The log level the logger will be logging.

        Returns:
            - : The logger.

        Raises:
            None.
        """
        # create logger with 'spam_application'
        logger = logging.getLogger('movies_ds')
        logger.setLevel(log_level)
        # create file handler
        fh = logging.FileHandler(log_path, mode='w')
        fh.setLevel(log_level)
        # Reset the logger.handlers if it already exists.
        if logger.handlers:
            logger.handlers = []
        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

        # add multiprocessing support for logging
        install_mp_handler(logger=logger)

        return logger


class TMDBFiller(DataFillerAbstract):
    def __init__(self, df: pd.DataFrame, fill_columns: dict, config_path: str = 'modules/config.ini',
                 log_path: str = 'log.txt', log_level: int = logging.INFO):
        assert os.path.exists(config_path)
        config = ConfigParser()
        config.read(config_path)
        tmdb.API_KEY = str(config.get('tmdb', 'token'))
        self.logger = TMDBFiller._get_logger(log_path, log_level)
        self.counter = defaultdict(int)

        self.movies_df = df.copy()

        # check all given columns are in the dataframe
        for col in fill_columns.keys():
            try:
                if col not in self.movies_df.columns:
                    raise ValueError(f'One of the fill column: {col} not in the dataframe columns: {self.movies_df.columns}')
            except ValueError as e:
                self.logger.error(str(e))
                raise

        self.fill_columns = fill_columns

    def fill(self, n_workers: int = cpu_count()) -> pd.DataFrame:
        self.logger.info(f'Using {n_workers} workers, filling is started.')
        # first filter rows that will be filled therefore we will process less rows
        # normalize the column list for filtering operation
        query_cols = {col: na_val for col, (_, na_val) in self.fill_columns.items()}
        # create query string with given column list. <NA> value is recognized as null and therefore
        # isna method is appended
        query = []
        for col, na_val in query_cols.items():
            if na_val == '<NA>' or na_val is None or pd.isna(na_val):
                query.append(f'{col}.isna()')
            else:
                query.append(f'{col}=={na_val}')

        # join query string list and filter using this query string
        query = ' | '.join(query)
        filtered_df = self.movies_df.query(query, inplace=False, engine='python')
        self.logger.info(f'Number of rows will be filled: {len(filtered_df)}')

        # for parallel processing, split, process and then concat
        df_split = np.array_split(filtered_df, n_workers)
        pool = Pool(n_workers)
        # fill_df = pd.concat(list(tqdm.tqdm(pool.imap(self._fill_func, df_split), total=len(filtered_df))))
        fill_df = pd.concat(pool.map(self._fill_func, df_split))
        # after concat, update original dataframe and preserve original dtypes
        original_dtypes = self.movies_df.dtypes
        self.movies_df.update(fill_df)
        # filtered_dtypes = self.movies_df.dtypes[self.movies_df.dtypes.index.isin(self.fill_columns.keys())]
        self.movies_df = self.movies_df.astype(original_dtypes)
        self.logger.info(f'Number of rows fully filled: ' 
                         f'{len(filtered_df) - len(self.movies_df.query(query, inplace=False, engine="python"))}'
                         f'There may be partially filled rows')
        pool.close()
        pool.join()

        return self.movies_df

    def _fill_func(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = [self._fill_row(len(df), index+1, row) for index, row in enumerate(df.itertuples())]
        return pd.DataFrame.from_dict(rows, orient='columns').set_index('index')

    def _fill_row(self, total: int, index: int, row: tuple) -> dict:
        row_dict = {'index': row.Index}
        curr_process_name = current_process().name
        will_fill_cols = []
        # Mark all the not available columns for this row
        for col, (_, na_val) in self.fill_columns.items():
            field_val = getattr(row, col)
            # if we need to fill NA values and the field is None
            if field_val is None or pd.isna(field_val):
                if na_val == '<NA>':
                    will_fill_cols.append(col)

            #   if the field not None, check whether it is equal or not
            else:
                if field_val == na_val:
                    will_fill_cols.append(col)

        if will_fill_cols:
            try:
                movie_id = getattr(row, 'id')
                movie = tmdb.Movies(movie_id)
                response = movie.info()
                self.logger.debug(f"[{index}/{total}]ID: {movie_id} Response: {response}")
            except requests.exceptions.HTTPError as errh:
                self.logger.error(f'Http Error:[{index}/{total}]ID: {movie_id} | {str(errh)}')
            except requests.exceptions.ConnectionError as errc:
                self.logger.error(f'Http Error:[{index}/{total}]ID: {movie_id} | {str(errc)}')
            except requests.exceptions.Timeout as errt:
                self.logger.error(f'Http Error:[{index}/{total}]ID: {movie_id} | {str(errt)}')
            except requests.exceptions.RequestException as err:
                self.logger.error(f'Http Error:[{index}/{total}]ID: {movie_id} | {str(err)}')
            else:
                # some fields may need a special treating so I have separated case by case
                # api fields name can be check here: https://developers.themoviedb.org/3/movies/get-movie-details
                for col in will_fill_cols:
                    original_col_name, _ = self.fill_columns[col]
                    val = getattr(movie, original_col_name, None)
                    if val is not None:
                        if original_col_name in ['adult', 'popularity', 'runtime', 'status',
                                                 'video', 'vote_average', 'vote_count']:
                            # may need special treatment later
                            pass
                        elif original_col_name in ['revenue', 'budget'] and int(val) > 0:
                            val = int(val)
                        elif original_col_name in ['tagline', 'release_date', 'original_language', 'original_title',
                                                   'backdrop_path', 'homepage', 'imdb_id',
                                                   'overview', 'poster_path', 'title'] and len(val) > 0:
                            val = str(val)
                        elif original_col_name is 'spoken_languages' and len(val) > 0:
                            val = val[0]['iso_639_1']

                        elif original_col_name in ['genres', 'production_companies',
                                                   'production_countries'] and len(val) > 0:
                            val = ", ".join((str(_val['name']) for _val in val))

                        elif original_col_name in ['belongs_to_collection']:
                            val = val['name']

                        else:
                            self.logger.error(f"[{curr_process_name}:{index}/{total}]ID: {movie_id} Name:{movie.title} "
                                              f"¦ Given field name: {original_col_name} for column name: {col} "
                                              f"is not valid or empty value, val:{val}")
                            continue

                        row_dict[col] = val
                        self.counter[col] += 1
                        self.logger.info(f"[{curr_process_name}:{index}/{total}]ID: {movie_id} Name:{movie.title} "
                                         f"¦ Missing field: {col} has been filled with: {val}")

                    else:
                        self.logger.info(f"[{curr_process_name}:{index}/{total}]ID: {movie_id} Name:{movie.title} "
                                         f"¦ Missing field: {col} cannot be filled")
        return row_dict






