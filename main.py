"""Pulls data from Google Search Console 25,000 rows at a time and loads it to BigQuery one calendar day at a a time.

Logging is done through print statements (Cloud Function preference)

Script checks last date written to BigQuery and starts iterating from there.
This typically takes 30 seconds per day and Cloud Functions has a 9 minute timeout.

Data is only pulled up to 4 days ago to avoid GSC limits where data may not appear for the last 72 hours

Google cloud credentials are inherited from the service account assigned to the cloud function at time of creation.
"""

from datetime import datetime, timedelta

from google.auth import credentials
from googleapiclient.discovery import build
import pandas as pd
from google.cloud import bigquery

def gsc_request_definition(start_row, dimensions, start_date, end_date, limit=25000):
    """Returns a valid GSC API request as a dict

    Parameters
    ----------
    start_row : int
        The table name to load data to in BQ
    dimensions : list of column names to request from GSC (current set in global variables)
    start_date : datetime
    end_date : datetime (start_date and end_date should always be the same in this job)
    limit: int
        The max number of records to fetch during each API poll
        GSC API max limit is 25000

    Returns
    -------
    request
        The completed request dict ready for GSC usage
    """
    startdate_string = start_date.strftime("%Y-%m-%d")
    enddate_string = end_date.strftime("%Y-%m-%d")
    
    # Create a request body
    return {
      'startDate': startdate_string,
      'endDate': enddate_string,
      'dimensions': dimensions, # hard coded to ensure no dataframe coercion mishaps
      'rowLimit': limit,
      'startRow': start_row
    }

def bq_table_definition(project_name, dataset_name, table_name):
    """Sets up the BigQuery client to be ready to load data to correct table

    Parameters
    ----------
    project_name : str
        the project name to load data to in BQ
    dataset_name : str
        The dataset name to load data to in BQ
    table_name : str
        The table name to load data to in BQ

    Returns
    -------
    job_config
        The completed job config to use for loading to BigQuery
    """

    # Create a job config
    job_config = bigquery.LoadJobConfig()

    # Set the destination table
    # table_ref = client.dataset(dataset_id).table(table_name)
    table_ref = f'{project_name}.{dataset_name}.{table_name}'

    # Update job config to reference the table & to append to existing data if any
    # job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_APPEND'

    return {
        "config": job_config,
        "table": table_ref
    }

def main(data, context) :
    ########### ENV Variables ####################################
    PROPERTY = 'okta.com'
    BQ_PROJECT_NAME = 'okta-ga-rollup'
    BQ_DATASET_NAME = 'search_console'
    BQ_TABLE_NAME = 'sc_daily'
    MAX_GSC_ROWS = 500000
    DIMENSIONS = ['date','page','query','device','country']
    ################ END OF ENV Variables ########################

    # local variables
    batch_size = 25000 #number of rows to request from Search Console at a time. 25000 is search console API max.
    lag_days = 4 #number of days before today to stop iteration. Gives Search Console some time to coalesce data.

    # Log into GSC and Setup Service
    print('## Setting Up Service for GSC')
    gsc_service = build('webmasters','v3')

    # Set up BigQuery Table and Client
    print('## Creating BigQuery Client')
    bq_client = bigquery.Client()

    print('## Creating BigQuery Table Definition')
    bq_config = bq_table_definition(client=bq_client, project_name=BQ_PROJECT_NAME, dataset_name=BQ_DATASET_NAME, table_name=BQ_TABLE_NAME)

    # get dates
    query_job = bq_client.query(f"SELECT max(date) FROM `{BQ_PROJECT_NAME}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}` where date >= '2021-08-01'")
    results = query_job.result()
    last_date = results.to_dataframe()
    check_date = last_date.iloc[0,0] + timedelta(days=1)
    end_date = datetime.now().date() - timedelta(days=lag_days)

    # Loop through all dates from start_date to end_date. Write to BQ once per date (no partial dates written to BQ).

    print('## Starting Daily Push to BigQuery')

    while check_date <= end_date :
        df = pd.DataFrame()
        for x in range(0, MAX_GSC_ROWS, batch_size):

            # create current request
            cur_request = gsc_request_definition(start_row=x, dimensions=DIMENSIONS, start_date=check_date, end_date=check_date, limit=batch_size)

            #get up to 25,000 records from GSC API
            print(f'## Fetching Analytics From GSC for {str(check_date)} rows {x} through {x+batch_size}')
            cur_fetch = gsc_service.searchanalytics().query(siteUrl=("sc-domain:" + PROPERTY), body=cur_request).execute()

            if len(cur_fetch) > 1:
                df = df.append(pd.DataFrame.from_dict(cur_fetch['rows']))
            
            if len(cur_fetch['rows']) < batch_size :
                print('## No More Records to Grab For This Date.')
                break

        # split the keys list into columns
        df[DIMENSIONS] = pd.DataFrame(df['keys'].values.tolist(), index=df.index)

        # Drop the key columns
        result = df.drop(['keys'],axis=1)

        # Add a website identifier
        result['website'] = PROPERTY

        # Change string date to datetime
        result['date'] = result['date'].astype('datetime64')

        # Load to BigQuery
        print(f'## Loading {len(result)} rows to BigQuery')
        
        load_job = bq_client.load_table_from_dataframe(dataframe=result, destination=bq_config["table"], job_config=bq_config["config"]).result()
        
        #increment check_date to restart loop
        check_date += timedelta(days=1)

    print('## All Dates Added to BigQuery')