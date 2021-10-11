import json
from logger import logger

class Utils:
    @staticmethod
    def get_config():
        """
            This method reads the config file
            :return: Dict -> the source file path dictionary
        """
        try:
            with open("../config", "r") as json_file:
                cfg = json.loads(json_file.read())
            files = cfg['source_file_path']
            return files
        except Exception as err:
            logger.error("%s, Error: %s", str(__name__), str(err))

    @staticmethod
    def load_csv(session,file_path):
        """
            This methods reads the CSV files
           :param session: SparkSession -> ~'pyspark.sql.SparkSession'
           :param file_path:  String -> CSV file path
           :return: DataFrame -> 'pyspark.sql.DataFrame'
        """
        try:
            data_df = session.read\
                .format('csv') \
                .option('mode','DROPMALFORMED')\
                .option('inferSchema','true')\
                .option('header','true')\
                .option('path',file_path)\
                .load()
            return data_df
        except Exception as err:
            logger.error("%s, Error: %s", str(__name__), str(err))

    @staticmethod
    def save_res(dataframe, output_path):
        """
            This method saves the resultant dataframe
            :param dataframe:  DataFrame -> `pyspark.sql.DataFrame`
            :param output_path: String -> Output path
            :return: None
       """
        try:
            dataframe.write\
            .format('csv')\
            .mode('overwrite')\
            .option('header','true')\
            .option('path', output_path)\
            .save()
            return "Success"
        except Exception as err:
            logger.error("%s, Error: %s", str(__name__), str(err))
