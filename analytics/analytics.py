from time import sleep

from utils.helper import *
from utils.logger import logger

def main():
    # Creating DBMS connections
    postgres_conn = create_dbms_engine('postgresql')
    mysql_conn = create_dbms_engine('mysql')

    # Grab the maximum id value in 'raw_analytics' table in mysql
    result = execute_query('max_id.sql', mysql_conn)
    if result is None:
        max_id = 0  # If "raw_analytics" is empty
    else:
        max_id = result

    # Fetch all records in Postgres 'devices' table greater than max_id
    df = read_sql_to_pandas('extract_postgres.sql',
                            postgres_conn, max_id)

    # Replace Null values in "next_loc" column with the corresponding "location" column values
    try:
        logger.info('Starting to replace "next_loc" null values ...')
        null_count = replace_nulls(df)
        logger.info(
            f'Successfully replaced {null_count} null values in "next_loc" column.')
    except Exception as e:
        logger.error(
            f'Exception occurred while replacing null "next_loc" values: {e}')

    # Convert "location" and "next_loc" column values into tuples of float
    try:
        logger.info(
            f'Converting "location" and "next_loc" columns to tuples ...')
        converted_count = [convert_coords_to_tuple(df, col) for col in [
            'location', 'next_loc']]
        logger.info(
            f'Successfully converted {converted_count} "location" and "next_loc" columns to tuples ...')
    except Exception as e:
        logger.error(
            f'Exception occurred while converting "location" and "next_loc" to tuples: {e}')

    # Calculate the distance between 2 consecutive locations in Km as "distance" column
    add_distance_column(df)

    # Find the total movement of the device over device_id and hour
    add_total_movement_column(df)

    # Load new records into "raw_analytics" table in MYSQL "main" database
    write_pandas_to_sql(df, mysql_conn, 'raw_analytics')

    # Fetch and aggregate new data from "raw_analytics"
    stg_analytics_df = read_sql_to_pandas('extract_raw_data.sql',
                                          mysql_conn, max_id)

    # Load and add new data into "stg_analytics" table in MYSQL database "analytics"
    write_pandas_to_sql(stg_analytics_df, mysql_conn, 'stg_analytics')

    # Fetch aggregated "stg_table" data into "analytics_df" dataframe
    analytics_df = read_sql_to_pandas('extract_stg_data.sql', mysql_conn)

    # Load and replace "analytics_df" data into "analytics1" table in MYSQL
    write_pandas_to_sql(analytics_df, mysql_conn, 'analytics')


if __name__ == '__main__':
    while True:
        logger.info('Waiting for the data generator...')
        sleep(120)
        logger.info('ETL Starting...')
        main()
