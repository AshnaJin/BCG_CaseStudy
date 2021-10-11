from pyspark.sql.functions import col
from utils import Utils

class TwoWheeler :
    """
     Analysis 2: How many two wheelers are booked for crashes?
    """
    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Integer

            Sample Output:
           +----------------------------------+
           | TOTAL_TWO_WHEELERS_CRASHES: 781  |
           +----------------------------------+
        """
        # fetch the path of units data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        # perform transformations
        two_wheeler_count = units_df.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE')).count()

        return two_wheeler_count

    @staticmethod
    def run(session, file):
        """
            Invokes the etl methods to get tha analysis report
            :param session: SparkSession -> Spark Session object
            :param file: Config
            :return: Integer -> Total No of crashes involving men dead
        """
        return TwoWheeler.__etl(TwoWheeler, session, file)
