'''
Unit tests for clean, remove_duplicates and aggregegation functions 
available in DataProcessor class.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dataprocess.dataprocessor import DataProcessor
import unittest


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession \
            .builder \
            .appName("test_data_processor") \
            .master('local[*]') \
            .getOrCreate()
        
        self.processor = DataProcessor(self.spark)

    def tearDown(self):
        self.spark.stop()


    def test_clean(self):
        '''
        Test the clean method by creating a DataFrame with null values 
        and verifying that null values are removed.

        Test Steps:
        1. Create a DataFrame with null values for testing.
        2. Call the 'clean' method on the test DataFrame to remove rows with null values.
        3. Check if the resulting DataFrame contains the expected number of rows.
        '''

        df_event = self.spark.createDataFrame(
            [(1, None, True, 1698913858), 
             (2, 'background','notDetermined',1698924658), 
             (None, 'refresh', '4', 1698931858)], 
             ['id', 'name', 'value', 'timestamp']
        )
        cleaned_df = self.processor.clean(df_event)
        self.assertEqual(cleaned_df.count(), 1)

    
    def test_remove_duplicates(self):
        '''
        Test the remove_duplicates method by creating a DataFrame 
        with duplicate records and verifying that duplicates are removed.

        Test Steps:
        1. Create a DataFrame with duplicate values for testing.
        2. Call the 'remove_duplicates' method on the test DataFrame to remove duplicate records.
        3. Check if the resulting DataFrame contains the expected number of rows after deduplication.
        '''

        df_event = self.spark.createDataFrame(
            [(1, 'background', 'notDetermined', 1698924658), 
             (1, 'background','notDetermined',1698924658), 
             (3, 'refresh', '4', 1698931858)], 
             ['id', 'name', 'value', 'timestamp']
        )
        deduplicated_df = self.processor.remove_duplicates(df_event)
        self.assertEqual(deduplicated_df.count(), 2)


    def test_aggregation(self):
        '''
        Test the aggregation method by creating a DataFrame 
        with test data and validating the aggregation process.

        Test Steps:
        1. Create a DataFrame with test data, including multiple rows and specified columns.
        2. Call the 'aggregation' method on the test DataFrame to perform data aggregation.
        3. Check if the resulting DataFrame contains the expected number of rows after aggregation.
        '''

        df_event = self.spark.createDataFrame(
            [(1,'notification','TRUE',1698913858),
             (3,'refresh','denied',1698921058),
             (2,'background','notDetermined',1698924658),
             (3,'refresh','4',1698931858),
             (1,'notification','FALSE',1698939058),
             (1,'background','TRUE',1698957058)], 
             ['id', 'name', 'value', 'timestamp']
        )
        aggregation_df = self.processor.aggregation(df_event, 'id, name', 'timestamp', 'name, value')
        self.assertEqual(aggregation_df.count(), 3)