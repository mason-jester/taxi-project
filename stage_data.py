import requests
import os
import logging

#instantiate a logger for stage_data
logger = logging.getLogger(__name__)

def check_directory(directory_name) -> str:
    """
    Checks that the directory_name exists in the current working directory

    If it does not exist, creates the directory_name
    """
    #create the target path from working directory and user input directory name
    target_path = os.path.join(os.getcwd(), directory_name)

    #check that the file staging folder exists
    #if it doesn't, create it
    if not os.path.exists(target_path):
         os.makedirs(target_path)
       
    return target_path

def create_dl_url(year:int, month:int, 
                  base_url:str = 'https://d37ci6vzurychx.cloudfront.net/trip-data/', car_type:str = 'yellow') -> str:
    
    """
    Creates the url from which to request files

    Defaults for base_url and car_type assume that we are interested in yellow taxi data
    """

    filename = f"{car_type}_tripdata_{year}-{month:02d}.parquet"

    download_url = base_url + filename

    return download_url, filename

def stage_data(year:int, month:int, car_type: str ="yellow", 
                 directory_name: str = 'file-staging', base_url: str = 'https://d37ci6vzurychx.cloudfront.net/trip-data/') -> bool:
    """
    Function to load data from NYC taxi source into a staging directory

    Uses check_directory to check that the target directory exists or create it if not

    Uses create_dl_url to choose the type of data to download + year/month
    """

    logger.info('Begin stage_data function...')

    staging_path = check_directory(directory_name)

    #check that year, month, and car_type are acceptable

    #will need to be in for loop eventually -> nevermind, put for loop in main
    download_url, filename = create_dl_url(year, month, car_type = car_type)
    write_path = os.path.join(staging_path, filename)

    max_tries = 3

    for attempt in range(max_tries):
        
        try:
            logger.info(f"Attempting download {attempt + 1}/{max_tries}: {filename}")
            #attempt to get the data
            response = requests.get(download_url, timeout= (10,30))
            #raises error for 4xx or 5xx response
            response.raise_for_status()

            #if we get here, we sucessfully got a response
            with open(write_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"Successfully downloaded {filename}")
            return True

        except requests.HTTPError as e:
            #404 error means we shouldn't retry url
            if response.status_code == 404:
                logger.error(f"File not found: {download_url}")
                return False
            
            #warn and try again
            else:
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")

        except requests.Timeout:
            logger.warning(f"Request timed out on attempt {attempt + 1}")

        except ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1}")

        #if we get an error with the writing of the file
        except IOError as e:
            logger.error(f"Error writing file {write_path}: {e}")
            #clean up partial file if it exists
            if os.path.exists(write_path):
                os.remove(write_path)
            return False
        
        #unexpected errors we don't handle
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            raise

    #if we make it here, we haven't sucesfully loaded the file after our max attempts
    logger.error(f"Failed to download {filename} after {max_tries} attempts")
    return False

