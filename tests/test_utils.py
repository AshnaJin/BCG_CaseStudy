from test_main import PySparkTest
from src.utils import Utils


class UtilsTest(PySparkTest):

    def test_load_csv(self):
        session = self.spark
        path = './test_files/test.csv'
        test_df = Utils.load_csv(session=session,file_path=path)
        self.assertEqual(5, test_df.count())

    def test_save(self):
        test_df = self.spark.createDataFrame([('bcg', 3), ('gamma', 2)]).toDF('name', 'rank')
        msg = Utils.save_res(test_df, 'test')
        self.assertEqual('Success', msg)
