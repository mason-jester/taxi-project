from stage_data import stage_data
import logging


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
    print("Hello from taxi-project!")
    setup_logging()

    #function to check for new data
    #check_for_data()

    #function to load data if we have detected new data
    stage_data(2024, 3, car_type = 'yellow')
    

if __name__ == "__main__":
    main()
