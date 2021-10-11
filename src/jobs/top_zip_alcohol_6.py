from pyspark.sql.functions import col
from utils import Utils


class AlcoholZip:
    """
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the
        contributing factor to a crash (Use Driver Zip Code)
    """
    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Dataframe

           Sample Output:
           +--------+-----+
           |DRVR_ZIP|count|
           +--------+-----+
           |   78521|   59|
           |   76010|   50|
           +--------+-----+
        """
        # fetch the path of units and primary person data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        primary_person_path = source_path + "/" + data_source['primary_person']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        primary_person_df = Utils.load_csv(session, primary_person_path)
        # perform transformations
        cars_df = units_df.filter(
            (col('VEH_BODY_STYL_ID') == 'SPORT UTILITY VEHICLE')
            | (col('VEH_BODY_STYL_ID').contains('CAR')))
        alcohol_influence = cars_df.filter((col('CONTRIB_FACTR_P1_ID') == 'UNDER INFLUENCE - ALCOHOL')
                                            | (col('CONTRIB_FACTR_1_ID') == 'UNDER INFLUENCE - ALCOHOL')
                                            | (col('CONTRIB_FACTR_2_ID') == 'UNDER INFLUENCE - ALCOHOL'))

        driver_df = primary_person_df.na.drop(subset=['DRVR_ZIP'])

        driver_alcohol = driver_df.join(alcohol_influence, driver_df.CRASH_ID == alcohol_influence.CRASH_ID, 'inner')\
            .select(driver_df.DRVR_ZIP, cars_df.CONTRIB_FACTR_P1_ID, alcohol_influence.CONTRIB_FACTR_1_ID,
                    alcohol_influence.CONTRIB_FACTR_2_ID, alcohol_influence.VEH_BODY_STYL_ID)

        top_zip_alcohol = driver_alcohol.groupBy("DRVR_ZIP")\
            .count()\
            .orderBy(col("count").desc())\
            .limit(5)

        return top_zip_alcohol

    @staticmethod
    def run(session, file):
        """
          Invokes the etl methods to get tha analysis report
          :param session: SparkSession -> Spark Session object
          :param file: Config
          :return: Dataframe -> Top Zip codes where alcohol was a contributing factor
        """
        return AlcoholZip.__etl(AlcoholZip, session, file)
