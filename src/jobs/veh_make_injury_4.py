import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils import Utils


class VehicleMake:
    """
      Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that
      contribute to a largest number of injuries including death
    """

    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Dataframe

            Sample Output:
           +---------------+-----------+----+
           |    VEH_MAKE_ID|TOTAL_COUNT|RANK|
           +---------------+-----------+----+
           |         NISSAN|       3118|   5|
           |          HONDA|       2892|   6|
           |            GMC|       1256|   7|
           |        HYUNDAI|       1103|   8|
           |            KIA|       1049|   9|
           |           JEEP|        989|  10|
           -----------------------------------
        """
        # fetch the path of units data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        # perform transformations
        total_inj = units_df \
            .groupBy('VEH_MAKE_ID') \
            .agg(F.sum(F.col('TOT_INJRY_CNT') + F.col('DEATH_CNT')).alias('TOTAl_COUNT'))

        veh_window = Window.orderBy(F.col('TOTAl_COUNT').desc())

        range_veh_make = total_inj \
            .withColumn('rank', F.row_number().over(veh_window)) \
            .filter(F.col('rank')
                    .between(5, 15))

        return range_veh_make

    @staticmethod
    def run(session, file):
        """
            Invokes the etl methods to get tha analysis report
            :param session: SparkSession -> Spark Session object
            :param file: Config
            :return: Dataframe -> Top vehicle make contributing to maximum injury
        """
        return VehicleMake.__etl(VehicleMake, session, file)
