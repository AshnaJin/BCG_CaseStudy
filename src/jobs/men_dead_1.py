from pyspark.sql.functions import col
from utils import Utils

class MaleDeaths:
    """
       Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male
   """

    def __etl(self, session, data_source):
        """
           :param session: SparkSession : `~pyspark.sql.SparkSession`
           :param data_source: Json config['source_file_path']
           :return: Integer

           Sample Output:
           +----------------------+
           | MEN_DEAD: 182        |
           +----------------------+
        """
        # fetch the path of primary person data file
        source_path = data_source['input_path']
        primary_person_path = source_path + "/" + data_source['primary_person']

        # extract the data into a DataFrame
        primary_person_df = Utils.load_csv(session, primary_person_path)
        # perform transformations
        male_dead = primary_person_df\
            .filter((col('PRSN_GNDR_ID')=='MALE') & (col('DEATH_CNT')>0))\
            .count()
        return male_dead


    @staticmethod
    def run(session, file):
        """
            Invokes the etl methods to get tha analysis report
            :param session: SparkSession -> Spark Session object
            :param file: Config
            :return: Integer -> Total No of crashes involving men dead
        """
        return MaleDeaths.__etl(MaleDeaths, session, file)
