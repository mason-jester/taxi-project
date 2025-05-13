import requests
import os
import logging

#instantiate a logger for stage_data
logger = logging.getLogger(__name__)

class TaxiDataLoader:
    """
    Class to load data form the NYC Taxi dataset
    Does not require any arguments on instantiation
    On instantiation checks that the /file-staging directory exists in current working directory. If not, creates it.
    """
    def __init__(self):
        #set up directories
        self.base_dir = os.getcwd()
        self.staging_dir = os.path.join(os.getcwd(), 'file-staging')

        #initialize logger
        self.logger = logging.getLogger(__name__)

        #this URL is shared for all taxi data downloads
        self.base_dl_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

        #see docstring
        self._ensure_directory()
    
    def _ensure_directory(self) -> str:
        """
        Checks that the staging_dir exists in the current working directory

        If it does not exist, creates the staging_dir ('file-staging')
        """

        if not os.path.exists(self.staging_dir):
            os.makedirs(self.staging_dir)
    
    def _ensure_inputs(self):
        #expected input types:
        expected_inputs = {'self': TaxiDataLoader, 'year': int, 'month': int, 'car_type': str, 'max_attempts': int}

        #some logic to check types of the inputs to download_file

        #if an input fails the type check, attempt to coerce it to the proper type

        #make car_type lower_case 

    def download_file(self, year: int, month: int, car_type: str, max_attempts: int = 3) -> bool:
        
        #example url:
        #https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

        #ensure that month is a 2 

        file_name = f"{car_type}_tripdata_{year}-{month:02d}.parquet"

        download_url = self.base_dl_url + file_name

        logger.info(f'Beginning file download from {download_url}')

        self.target_file = os.path.join(self.staging_dir, file_name)


        for attempt in range(max_attempts):

            try:
                logger.info(f"Attempting download {attempt + 1}/{max_attempts}: {download_url}")
                #attempt to get the data
                response = requests.get(download_url, timeout= (10,30))
                #raises error for 4xx or 5xx response
                response.raise_for_status()

                #if we get here, we sucessfully got a response
                with open(self.target_file, 'wb') as f:
                    f.write(response.content)

                logger.info(f"Successfully downloaded {download_url}")
                return True

            #ERROR HANDLING
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
                logger.error(f"Error writing file {self.staging_dir}: {e}")
                #clean up partial file if it exists
                if os.path.exists(self.target_file):
                    os.remove(self.target_file)
                return False
            
            #unexpected errors we don't handle
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                raise

        #if we make it here, we haven't sucesfully loaded the file after our max attempts
        logger.error(f"Failed to download {download_url} after {max_attempts} attempts")

        return False
