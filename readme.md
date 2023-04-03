## Package Description
Pulls data from StackAdapt, starting from the latest date in the BigQuery table and running through yesterday and loads that data into Google BigQuery. Written to BQ one day at a time (no partial days written to BQ).

BigQuery loads data into the KanopiByArmstrong project under the `stackadapt` dataset.

Connection to GSC and BigQuery are made through service account "<fill in later>" using Google Cloud native authentication. Job runs in a Cloud Function with the creator designated as the service account.

## Environment Setup Needed:
This package runs in a Python Virtual Environment, using Python 3.7.5 and the Python Virtual Environment Package Manager called **pipenv**

The package is intended to run on a Linux Operating System, with Ubuntu 18.04 as the target distribution.

Ensure the following base setup:
- Python 3.7
- requirements.txt file to install external packages

## Environment Variables and Deploying to GCP
The job lives in the Google Cloud Function "stackadapt_etl" in the project "KanopiByArmstrong".

## Scheduling daily pull
Cloud Function is set to trigger on Pub/Sub with topic "maketing_analytics_function_trigger".

Pub/Sub fires every morning at 3am PDT.