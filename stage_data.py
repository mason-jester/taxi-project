import requests
import os
from datetime import datetime, timedelta
import sys
import pyarrow.parquet as pq
import pandas as pd

def check_directory(directory_name) -> str:
    """
    Checks that the directory_name exists in the current working directory
    If it does not exist, creates the directory_name
    """
    #create the target path from working directory and user input directory name
    target_path = os.path.join(os.getcwd(), directory_name)

    #check that the file staging folder exists
    if os.path.exists(target_path):
        pass
    else:
        os.makedirs(target_path)
    
    return target_path

def create_dl_url(year:int, month:int, 
                  base_url: str = 'https://d37ci6vzurychx.cloudfront.net/trip-data/', car_type: str = 'yellow') -> str:
    
    """
    Creates the url from which to request files
    Defaults for base_url and car_type assume that we are interested in yellow taxi data
    """

    filename = f"{car_type}_tripdata_{year}-{month:02d}.parquet"

    download_url = base_url + filename

    return download_url, filename

def collect_data(year:int, month:int, car_type: str ="yellow", 
                 directory_name: str = 'file-staging', base_url: str = 'https://d37ci6vzurychx.cloudfront.net/trip-data/') -> None:
    """
    Function to create the URL with which to download the parquet file from the NYC Taxi dataset

    Then downloads file from URL

    And places it in file-staging folder
    """

    staging_path = check_directory(directory_name)

    #check that year, month, and car_type are acceptable

    #will need to be in for loop eventually
    download_url, filename = create_dl_url(year, month, car_type)
    write_path = os.path.join(staging_path, filename)

    try:
        response = requests.get(download_url)

        print(response.status_code)
    
    except:
        print(f'{download_url} returned error')


    with open(write_path, 'wb') as f:
        f.write(response.content)
    
    return

