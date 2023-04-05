"""
Pulls data from an api and inserts into BigQuery.

The module api_getter takes in a start_date and optional end_date and returns a pandas dataframe with all dtypes scrubbed.

This is meant to be a fairly generic script. Replacing the contents of api_getter persuant to whatever api is called would let this script
be deployed toward any future api.

If the table doesn't exist in BigQuery, the first run will create it.

Google Cloud Functions will start the chain of events with the main function.
"""

import os
import api_getter
import pandas as pd
from google.cloud import bigquery
from datetime import timedelta, date

def get_last_synced_date(client, table_name) :
    query = f"""
    SELECT MAX(date) AS max_date
    FROM `{table_name}`
    """

    # Make the query and get the result
    result = client.query(query).result()

    # Get the max date from the result
    max_date = next(result).get('max_date')
    return max_date

def delete_last_synced_date(client, table_name, date:str) :
    query = f"""
    DELETE
    FROM `{table_name}`
    WHERE date >= '{date}'
    """

    # Make the query and get the result
    result = client.query(query).result()
    return

def main() :
    project_id = "kanopibyarmstrong"
    dataset_id = "stackadapt"
    table_id = "stackadapt_daily"

    table_name = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client()

    print('## querying last synced date')
    try:
        last_synced_date = get_last_synced_date(client, table_name)
    except:
        last_synced_date = date(2021,2,1) #change this date based on the earliest known available data in the API.

    start_date = last_synced_date - timedelta(1)
    start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')

    print('## pulling api data')
    df = api_getter.get_incremental_data(start_date)

    print('## deleting the most recently synced date from BigQuery')
    delete_last_synced_date(client, table_name, start_date)

    print('## inserting rows from the api pull into BigQuery')
    df.to_gbq(destination_table = f"{dataset_id}.{table_id}", project_id = project_id, chunksize = 10000, if_exists='append')