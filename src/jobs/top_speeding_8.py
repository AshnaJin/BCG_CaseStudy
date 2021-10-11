from pyspark.sql.functions import col
from utils import Utils

class TopSpeeding:
    """
       Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
       has licensed Drivers, uses top 10 used vehicle colours
       and has car licensed with the Top 25 states with highest
       number of offences (to be deduced from the data)
    """

    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Dataframe `~spark.sql.Dataframe`

            Sample Output:
            |VEH_MAKE_ID|
            +-----------+
            |       FORD|
            |      HONDA|
            |     SUBARU|
            |    HYUNDAI|
            ------------
        """

        # fetch the path of units, endorse and charges data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        charges_path = source_path + "/" + data_source['charges_use']
        endorse_path = source_path + "/" + data_source['endorse_use']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        charges_df = Utils.load_csv(session, charges_path)
        endorse_df = Utils.load_csv(session, endorse_path)
        # perform transformations

        # top 10 used vehicle colors
        top_color = units_df.groupBy('VEH_COLOR_ID').count().orderBy(col('count').desc()).limit(10)
        color_list = [row[0] for row in top_color.select('VEH_COLOR_ID').collect()]

        # Top 25 states with highest number of offences
        top_state = units_df.groupBy('VEH_LIC_STATE_ID').count().orderBy(col('count').desc()).limit(25)
        state_list = [row[0] for row in top_state.select('VEH_LIC_STATE_ID').collect()]

        # drivers are charged with speeding related offences
        speed = charges_df.join(endorse_df, 'CRASH_ID').drop(endorse_df.CRASH_ID)\
            .filter((col('CHARGE').contains('SPEED'))
                    & (~col('DRVR_LIC_ENDORS_ID').isin(['UNKNOWN', 'UNLICENSED', 'NONE'])))

        required_columns = speed.join(units_df, 'CRASH_ID')\
            .select(speed.CRASH_ID, units_df.VEH_COLOR_ID, units_df.VEH_LIC_STATE_ID, units_df.VEH_MAKE_ID)\
            .filter(units_df.VEH_MAKE_ID != 'NA')
        top_speeding_make = required_columns\
            .filter((col('VEH_LIC_STATE_ID').isin(state_list))
                    & (col('VEH_COLOR_ID').isin(color_list)))\
            .groupBy('VEH_MAKE_ID').count().orderBy(col('count').desc()).limit(5)

        return top_speeding_make


    @staticmethod
    def run(session, file):
        """
             Invokes the etl methods to get tha analysis report
             :param session: SparkSession -> Spark Session object
             :param file: Config
             :return: DataFrame -> Top speeding vehicle according to color and state
        """
        return TopSpeeding.__etl(TopSpeeding, session, file)
