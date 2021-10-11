from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from utils import Utils


class BodyStyleGroup:
    """
       Analysis 5: For all the body styles involved in crashes,
       mention the top ethnic user group of each unique body style
    """

    def __etl(self, session, data_source):
        """
            :param session: SparkSession : `~pyspark.sql.SparkSession`
            :param data_source: Json config['source_file_path']
            :return: Dataframe

            Sample Output:
            +----------------+----------------+
            |VEH_BODY_STYL_ID|PRSN_ETHNICITY_ID
            +---------------+-----------------+
            |      AMBULANCE|            WHITE|
            |            BUS|            WHITE|
            |  FARM EQUPMENT|            WHITE|
            -----------------------------------
        """
        # fetch the path of units and primary person data file
        source_path = data_source['input_path']
        units_path = source_path + "/" + data_source['units_use']
        primary_person_path = source_path + "/" + data_source['primary_person']
        # extract the data into a DataFrame
        units_df = Utils.load_csv(session, units_path)
        primary_person_df = Utils.load_csv(session, primary_person_path)
        # perform transformations

        body_style_win = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())

        user_group = units_df.join(primary_person_df, units_df.CRASH_ID == primary_person_df.CRASH_ID, 'inner') \
            .select(units_df.CRASH_ID, units_df.VEH_BODY_STYL_ID, primary_person_df.PRSN_ETHNICITY_ID) \
            .filter(
            col('VEH_BODY_STYL_ID').isin('NA', 'NOT REPORTED', 'UNKNOWN', 'OTHER (EXPLAIN IN NARRATIVE)')==False)

        body_style = user_group.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()
        top_ethnic_user = body_style\
            .withColumn('rnk', row_number().over(body_style_win))\
            .filter(col('rnk') == 1)\
            .drop('rnk', 'count')

        return top_ethnic_user

    @staticmethod
    def run(session, file):
        """
            Invokes the etl methods to get tha analysis report
            :param session: SparkSession -> Spark Session object
            :param file: Config
            :return: Dataframe -> Top ethnic group using a particular vehicle body style
        """
        return BodyStyleGroup.__etl(BodyStyleGroup, session, file)
