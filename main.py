from stage_data import TaxiDataLoader
import logging

#configure logging
def setup_logging():
    """
    Configure logging for the entire pipeline
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('taxi_pipeline.log'),
            #allows console output
            logging.StreamHandler()
        ]
    )

def main():
    setup_logging()

    #function to check for new data
    #check_for_data()

    #function to load data if we have detected new data
    dataLoader = TaxiDataLoader()

    dataLoader.download_file(2023, 1, 'yellow', 3)

if __name__ == "__main__":
    main()
