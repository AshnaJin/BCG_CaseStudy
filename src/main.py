import argparse
from logger import logger
from pyspark.sql import SparkSession, DataFrame
from utils import Utils
from jobs import MaleDeaths, TwoWheeler,TopState, VehicleMake,BodyStyleGroup, AlcoholZip, NoPrptDamage, TopSpeeding

if __name__ == '__main__':

    # To get the required arguments from command line
    parser = argparse.ArgumentParser(description='BCG Gamma Case Study')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help='Provide the name of the job you want to run')
    args = parser.parse_args()

    # To get the class object of the given job
    job_module = args.job_name
    class_list = {
        "men_dead_1": MaleDeaths,
        "two_wheeler_2": TwoWheeler,
        "top_state_female_3": TopState,
        "veh_make_injury_4": VehicleMake,
        "top_ethnic_group_5": BodyStyleGroup,
        "top_zip_alcohol_6": AlcoholZip,
        "no_prpt_damage_7": NoPrptDamage,
        "top_speeding_8": TopSpeeding
    }
    logger.debug("Started the job - %s ", str(job_module).upper())

    try:
        # Start the spark session
        spark = SparkSession\
                .builder\
                .appName('BCG_CaseStudy_CarCrash')\
                .getOrCreate()
        # To get the path from config file
        data_source = Utils.get_config()
        result = class_list[job_module].run(session=spark, file=data_source)
        # Save the resultant dataframe or string or integer value
        if isinstance(result, DataFrame):
            Utils.save_res(result, data_source['output_file_path'])
        else:
            print(f'{str(job_module).upper()}: {result}')
    except Exception as err:
        logger.error("%s Error : %s", __name__, str(err))
    finally:
        # stop the spark session at the end to the execution
        spark.stop()
        logger.debug("Successfully stopped spark session ")

