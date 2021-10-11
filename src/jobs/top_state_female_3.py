from pyspark.sql.functions import col,count
from utils import Utils

class TopState:
    """
        Analysis 3: Which state has highest number of accidents in which females are involved?
    """
    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Dataframe

            Sample Output:
           +-----------------+-----+
           |DRVR_LIC_STATE_ID|count|
           |            TEXAX|53319|
           +-----------------+-----+
         """

        # fetch the path of primary person data file
        source_path = data_source['input_path']
        primary_person_path = source_path + "/" + data_source['primary_person']

        # extract the data into a DataFrame
        primary_person_df = Utils.load_csv(session, primary_person_path)
        # perform transformations
        top_state_female = primary_person_df\
            .filter(col('PRSN_GNDR_ID') == 'FEMALE')\
            .groupBy(col('DRVR_LIC_STATE_ID'))\
            .agg(count(col('CRASH_ID')).alias('TOTAL_CRASHES'))\
            .orderBy(col('TOTAL_CRASHES').desc())\
            .first()
        return top_state_female


    @staticmethod
    def run(session, file):
        """
           Invokes the etl methods to get tha analysis report
           :param session: SparkSession -> Spark Session object
           :param file: Config
           :return: Dataframe -> Total No of crashes by female per state
        """
        return TopState.__etl(TopState, session, file)
