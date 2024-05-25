import pandas as pd

from os import environ
from typing import List, Optional, Tuple

from geopy.distance import distance

import sqlalchemy.engine.base
from sqlalchemy.types import *
from sqlalchemy.exc import ResourceClosedError, OperationalError, ProgrammingError
from sqlalchemy import create_engine, text

from sqlalchemy.orm import sessionmaker

from utils.logger import logger


def create_dbms_engine(dbms_name: str) -> Optional[sqlalchemy.engine.base.Engine]:
    """Creates SQLAlchemy engine for the given dbms name."""
    while True:
        try:
            logger.info(
                f'Trying to connect to "{dbms_name.lower().capitalize()}" ...')
            engine = create_engine(
                environ[f'{dbms_name.upper()}_CS'], pool_pre_ping=True, pool_size=10)
            break
        except OperationalError as e:
            logger.error(
                f'Exception occurred during "{dbms_name.lower().capitalize()}" connection creation: {e}')

    logger.info(
        f'Connection to "{dbms_name.lower().capitalize()}" successful.')
    return engine


def execute_query(sql_file: str,
                  connection: sqlalchemy.engine.base.Engine,
                  max_id: Optional[int] = None) -> Optional[int]:
    """Executes the given SQL query as the sql_file string content on the given connection."""

    try:
        logger.info(f'Opening the file "{sql_file}" ...')
        file_path = f'/app/sql_queries/{sql_file}'
        with open(file_path, 'r') as f:
            query = f.read()
            if sql_file == 'extract_raw_data.sql':
                query = query.format(max_id)
    except Exception as e:
        logger.error(f'Exception occurred while reading "{sql_file}": {e}')

    try:
        with connection.connect() as conn:
            logger.info('Starting query execution...')
            result = conn.execute(text(query)).fetchall()
            if sql_file == 'max_id.sql':
                return result[0][0]
            logger.info(f'Query execution completed, successfully.')
    except ResourceClosedError as e:
        logger.warn(f'SQL query execution compelted without any output: {e}')
    except OperationalError as err:
        logger.error(
            f'Exception occurred while executing the query "{sql_file}": {err}')
    except ProgrammingError:
        logger.warn('There were no data in "main" database...')
        pass


def read_sql_to_pandas(sql_file: str,
                       connection: sqlalchemy.engine.base.Engine,
                       max_id: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Executes the given SQL query as the sql_file string content on the given connection.
    The grabbed data is then stored into a pandas dataframe.

    If the sql_file is "extract_postgres.sql", then it will extract the table "devices" records
    in the main database in psql_db host.
    The extracted records are those with "id" greater than the maximum "id" value in the table
    "raw_analytics" in the analytics1 database in MYSQL DBMS.
    """
    global df
    table = 'stg_analytics'
    dataframe = 'analytics_df'
    try:
        logger.info(f'Opening the file "{sql_file}" ...')
        file_path = f'/app/sql_queries/{sql_file}'
        with open(file_path, 'r') as f:
            query = f.read()
            if sql_file == 'extract_postgres.sql':
                query = query.format(max_id)
                table = 'devices'
                dataframe = 'df'
            elif sql_file == 'extract_raw_data.sql':
                query = query.format(max_id)
                table = 'raw_analytics'
                dataframe = 'stg_analytics_df'
    except Exception as e:
        logger.error(f'Exception occurred while reading "{sql_file}": {e}')

    try:
        logger.info(
            f'Starting extracting "{table}" table records into "{dataframe}" dataframe...')
        df = pd.read_sql_query(query, connection)

        logger.info(
            f'Successfully loaded {len(df)} records into "{dataframe}" dataframe.')
    except OperationalError as err:
        logger.error(
            f'Exception occurred while executing the query "{sql_file}": {err}')

    return df


def replace_nulls(dataframe: pd.DataFrame) -> int:
    """Replace null values in "next_loc" column with corresponding "location" column values."""
    idx = dataframe[dataframe['next_loc'].isnull()].index.tolist()
    dataframe.loc[dataframe['next_loc'].isnull(), 'next_loc'] = dataframe.loc[
        dataframe['next_loc'].isnull(), 'location']

    return len(idx)


def to_tuple(coord: dict) -> Optional[Tuple[float, float]]:
    """converts dictionary of strings to tuple of floats"""
    return float(coord['latitude']), float(coord['longitude'])


def convert_coords_to_tuple(dataframe: pd.DataFrame, col: str) -> int:
    """converts 'location' and 'next_loc' columns to tuple of floats"""
    dataframe[col] = dataframe[col].apply(to_tuple)

    return len(dataframe[col])


def calculate_distance(coord1: dict, coord2: dict) -> float:
    """calculates the distance in "Km"  between two points using "geopy.distance" module"""
    return round(distance(coord1, coord2).km, 3)


def add_distance_column(dataframe: pd.DataFrame):
    """Creates the "distance" column as the distance between 'location' and 'next_loc' columns."""
    try:
        logger.info('Creating "distance" column ...')
        dataframe.insert(loc=7,
                         column='distance',
                         value=dataframe.apply(
                             lambda x: calculate_distance(
                                 x['location'], x['next_loc']),
                             axis=1))

        distance_count = len(dataframe['distance'])
        logger.info(
            f'Successfully created "distance" column with {distance_count} rows.')
    except Exception as e:
        logger.error(
            f'Exception occurred while calculating "distance" values: {e}')


def add_total_movement_column(dataframe: pd.DataFrame):
    try:
        logger.info('Creating "total_movement" column')
        dataframe['total_movement'] = dataframe.groupby(by=['device_id', 'timestamp'])[
            'distance'].transform('sum')

        total_movement_count = len(dataframe['total_movement'])
        logger.info(
            f'Successfully created "total_movement" column with {total_movement_count} rows.')
    except Exception as e:
        logger.error(
            f'Exception occurred while calculating "total_movement" values: {e}')


def write_pandas_to_sql(dataframe: pd.DataFrame,
                        connection: sqlalchemy.engine.base.Engine,
                        table: str):
    """
    Writes pandas dataframe to MSQL "analytics1" database.

    The table schema "raw_analytics" in "analytics1" database in MYSQL DBMS is specified by
    "df_schema" dictionary, as "dtype" parameter in to_sql method.
    To add data into "raw_analytics" table, new data is appended.

    The final "analytics1" table has the schema "analytics_df_schema" and the old records
    are replaced by new ones.
    """
    df_schema = {
        "id": BIGINT,
        "device_id": CHAR(36),
        "epoch": BIGINT,
        "timestamp": CHAR(12),
        "temperature": INTEGER,
        "location": JSON,
        "next_loc": JSON,
        "distance": DECIMAL(50, 3),
        "max_dev_temp": INTEGER,
        "dev_data_count": BIGINT,
        "total_movement": DECIMAL(50, 3)
    }

    analytics_df_schema = {
        "device_id": CHAR(36),
        "timestamp": CHAR(12),
        "max_dev_temp": INTEGER,
        "dev_data_count": BIGINT,
        "total_movement": DECIMAL(50, 3)
    }
    method = 'append'
    schema = df_schema


    try:
        logger.info(
            f'Starting to load dataframe data into MYSQL table "{table}"...')
        if table == 'analytics':
            method = 'replace'
            schema = analytics_df_schema
        if table == 'stg_analytics':
            schema = analytics_df_schema
        dataframe.to_sql(
            table,
            con=connection,
            if_exists=method,
            index=False,
            dtype=schema)
        logger.info(
            f'Successfully loaded {len(dataframe)} records into MYSQL table "{table}"')

    except OperationalError as err:
        logger.error(
            f'Exception occurred while loading dataframe data into MYSQL table "{table}": {err}')
