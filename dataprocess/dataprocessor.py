from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, collect_list, map_from_entries, row_number
from pyspark.sql.window import Window


class DataProcessor:
    def __init__(self, spark_session: SparkSession):
        '''
        Initialize a DataProcessor instance.

        Parameters:
            spark_session (SparkSession): The Spark session to be used for processing.
        '''

        self.spark_session = spark_session

    
    def clean(self, df_event: DataFrame) -> DataFrame:
        '''
        Cleans a Spark DataFrame by identifying and removing rows with null values.

        Parameters:
            df_event (DataFrame): The input Spark DataFrame to be cleaned.

        Returns:
            DataFrame: A cleaned Spark DataFrame with rows containing null values removed.
        '''

        null_counts = [df_event.filter(col(column).isNull()).count() for column in df_event.columns]

        for column, count in zip(df_event.columns, null_counts):
            if count > 0:
                print(f"Column '{column}' has {count} null values")

        return df_event.na.drop()


    def remove_duplicates(self, df_event: DataFrame) -> DataFrame:
        '''
        Removes duplicate records from a Spark DataFrame based on all columns.

        Parameters:
            df_event (DataFrame): The input Spark DataFrame from which duplicates will be removed.

        Returns:
            DataFrame: A Spark DataFrame with duplicate records removed.
        '''

        duplicate_counts = df_event.groupBy(df_event.columns).count().filter(col("count") > 1)
        print("Duplicate record count: " + str(duplicate_counts.count()))

        return df_event.dropDuplicates()
    

    def aggregation(self, df_event: DataFrame, partition_cols: str, order_col: str, aggregation_cols: str) -> DataFrame:
        '''
        Aggregates data in a Spark DataFrame by partitioning, ranking, and collecting specified columns.

        Parameters:
            df_event (DataFrame): The input Spark DataFrame to be aggregated.
            partition_cols (str): Comma-separated column names to partition by.
            order_col (str): The column to order by for ranking.
            aggregation_cols (str): Comma-separated column names to aggregate.

        Returns:
            DataFrame: A Spark DataFrame with aggregated data.
        '''
        try:
            partition_cols_list = [value.strip() for value in partition_cols.split(',')]
            partition_columns = [col(col_name) for col_name in partition_cols_list]
            window_spec = Window.partitionBy(*partition_columns).orderBy(col(order_col).desc())

            rank_events = df_event.withColumn("rank", row_number().over(window_spec))
            latest_event = rank_events.filter(col("rank") == 1)

            aggregation_cols_list = [value.strip() for value in aggregation_cols.split(',')]
            aggregation_columns = [col(col_name) for col_name in aggregation_cols_list]
            agg_df = latest_event.groupBy("id").agg(
                collect_list(struct(*aggregation_columns)).alias("settings")
            )

            return agg_df.withColumn("settings", map_from_entries(col("settings")))
        
        except Exception as e:
            print(str(e))
            return None