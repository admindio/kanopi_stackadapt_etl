"""
A series of functions that gather data from an API and returns a pandas dataframe.

The main function to call is get_incremental_data(). This takes in a start date and optional end_date. (Both string
 YYYY-MM-DD).

This iteration of api_getter is set up for StackAdapt ad network on behalf of KanopiByArmstrong
"""

from datetime import datetime, timedelta, date
from time import sleep
import requests
import os
import pandas as pd
from google.cloud import bigquery

api_key = os.environ["STACKADAPT_API_KEY"]

def get_campaigns (api_key) :
    clean_campaigns = []
    page = 1
    total_campaigns=1
    while total_campaigns > len(clean_campaigns) :
        campaigns, total_campaigns = get_campaign_page(api_key, page)
        clean_campaigns.extend(campaigns)
        page += 1
    return clean_campaigns

def get_campaign_page (api_key, page=1) :
    payload = {'page': page}
    url = f"https://api.stackadapt.com/service/v2/campaigns"

    headers = {'X-Authorization': api_key}
    clean_campaigns = []
    response = requests.request("GET", url, headers=headers, params=payload)
    for c in response.json()['data'] :
        cc = {key: c[key] for key in ['id','name','created_at','updated_at','state','bid_type','bid_amount_total','start_date','end_date','channel','campaign_type','budget']}
        clean_campaigns.append(cc)
    
    return clean_campaigns, response.json()['total_campaigns']

def get_delivery_stats(api_key, campaign: dict, start_date = None, end_date = None) :
    if start_date == None :
        start_date = campaign['start_date']
    if end_date == None :
        end_date = str(date.today() - timedelta(1))
        
    payload = {
        'resource_type': 'campaign',
        'type': 'daily',
        'id': campaign['id'],
        'date_range_type': 'custom',
        'start_date': start_date,
        'end_date': end_date,
        'group_by_resource': 'native_ad'
              }
    url = "https://api.stackadapt.com/service/v2/delivery"

    headers = {'X-Authorization': api_key}
    response = requests.request("GET", url, headers=headers, params=payload)
    
    stats = response.json()['stats']
    
    clean_stats = []
    
    try: 
        for d in stats :
            dd = {key: d[key] for key in ['click','cost','conv','date','imp','unique_imp','uniq_conv','click'] if key in d}
            dd.update(campaign)
            clean_stats.append(dd)
    except:
        print(f"campaign_id {campaign['id']} returned zero results.")

    return clean_stats

def get_incremental_data(start_date, end_date=None) :
	campaigns = get_campaigns(api_key)
	combined_stats = []
	counter = 1
	for c in campaigns :
	    stats = get_delivery_stats(api_key, c, start_date, end_date)
	    combined_stats.extend(stats)
	    print(f"{counter} of {len(campaigns)} campaigns processed")
	    sleep(.1)
	    counter += 1

	df = pd.DataFrame(combined_stats).fillna(0)
	df = df.astype({
		'unique_imp':'int64',
		'bid_amount_total':'int64',
		'date': 'datetime64[ns]',
		'start_date': 'datetime64[ns]',
		'end_date': 'datetime64[ns]',
		'created_at': 'datetime64[ns]',
		'updated_at': 'datetime64[ns]'
		})
	return df

