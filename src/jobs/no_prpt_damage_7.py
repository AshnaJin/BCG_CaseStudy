from pyspark.sql.functions import col,countDistinct, split
from pyspark.sql.types import IntegerType
from utils import Utils

class NoPrptDamage:
    """
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property
        was observed and Damage Level (VEH_DMAG_SCL~) is
        above 4 and car avails Insurance
    """
    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Integer

            Sample Output:
            +-----------------------------------+
            |NO_PRPT_DAMAGE_CRASHES  : 1934     |
            -------------------------------------
        """
        # fetch the path of units and damages data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        damages_path = source_path + "/" + data_source['damages_use']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        damages_df = Utils.load_csv(session, damages_path)
        # perform transformations
        crash_data = units_df.join(damages_df, 'CRASH_ID','left_outer')

        crash_df = crash_data\
            .withColumn('VEH_DMAG_SCL_1', split(col('VEH_DMAG_SCL_1_ID'), ' ').getItem(1).cast(IntegerType()))\
            .withColumn('VEH_DMAG_SCL_2', split(col('VEH_DMAG_SCL_2_ID'), ' ').getItem(1).cast(IntegerType()))

        crash_greater_4 = crash_df.filter((col('VEH_DMAG_SCL_1') > 4) | (col('VEH_DMAG_SCL_2') > 4))
        crash_insurance = crash_greater_4.filter((col('DAMAGED_PROPERTY').isNull())
                                     & (col('FIN_RESP_TYPE_ID').contains('INSURANCE')))

        no_prpt_damage = crash_insurance.agg(countDistinct('CRASH_ID').alias('CRASH_ID_COUNT'))

        return no_prpt_damage


    @staticmethod
    def run(session, file):
        """
            Invokes the etl methods to get tha analysis report
            :param session: SparkSession -> Spark Session object
            :param file: Config
            :return: Int -> Total No of crashes involving no property damage
        """
        return NoPrptDamage.__etl(NoPrptDamage, session, file)
