'''
spark version 3.5.0
Assumptios:
    - Event data stored in a file path called event_data 
       (For example this can be HDFS path, AWS S3 location etc.)
    - Daily event data store in a seperate file with a file name as a current date 
      (For example csv file with DDMMYYYY.csv format)
    - Output data stored in Parquet format (Parquet is a columnar storage file format, 
        storing data by column rather than by row. This enables better compression and encoding of data within columns, 
        resulting in reduced disk I/O and faster query performance.)
    - In production this should be run on yarn resource manager (This ensures that resources are allocated efficiently 
        and fairly among different applications.)
    - spark-submit num-executors, executor-memory should be carefully selected.
'''

import os
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark import StorageLevel

from dataprocess.configloader import ConfigLoader
from dataprocess.dataprocessor import DataProcessor


# Get base directory path
base_path = os.path.dirname(__file__)

# Get config file
config_file = os.path.join(base_path, 'config/config.ini')

# Create a ConfigParser object
config_loader = ConfigLoader(config_file)

# Access configuration values
partition_cols = config_loader.get_config_value('TABLE', 'partition_cols')
order_col = config_loader.get_config_value('TABLE', 'order_col')
aggregation_cols = config_loader.get_config_value('TABLE', 'aggregation_cols')
file_path = config_loader.get_config_value('PATH', 'file_path')
output_path = config_loader.get_config_value('PATH', 'output_path')
log_path = config_loader.get_config_value('PATH', 'log_path')
mode = config_loader.get_config_value('SPARK', 'mode')

# Get current date
today = date.today()
formatted_date = today.strftime('%d%m%Y')

# Get log path
log_path = os.path.join(base_path, log_path)

# Redirect print statements into log file (This creates daily log files)
log_file = open("{}{}.log".format(log_path, formatted_date), 'a')
sys.stdout = log_file


if __name__ == '__main__':

    print("---------------" + str(today) + "---------------")

    # Get event data file path
    input_path = os.path.join(base_path, file_path)

    # Check all the configurations
    if partition_cols is None or \
        order_col is None or \
        aggregation_cols is None or \
        file_path is None or \
        output_path == None:
        
        sys.exit(1)


    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName('Transformer') \
        .master(mode) \
        .getOrCreate()

    data_processor = DataProcessor(spark)

    # Create schema
    schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('name', StringType(), nullable=True),
                StructField('value', StringType(), nullable=True),
                StructField('timestamp', LongType(), nullable=False)
            ])


    # Check daily file availability
    if not os.path.exists("{}{}.csv".format(input_path, formatted_date)):
        print("File not found in " + input_path)
        sys.exit()

    # Read data and create spark dataframe
    try:
        df_event = spark.read \
            .format('csv') \
            .schema(schema) \
            .load("{}{}.csv".format(input_path, formatted_date))

    except Exception as e:
        print(str(e))
        sys.exit()


    # Remove null values from dataframe
    df_event = data_processor.clean(df_event)

    # Remove duplicate records
    df_event = data_processor.remove_duplicates(df_event)

    # Repartition using id column as id column is widely use for the aggregations
    df_event = df_event.repartition("id")
    print("Number of partitions: " + str(df_event.rdd.getNumPartitions()))

    # Cache dataframe (MEMORY_AND_DISK - if the data does not fit in memory, it spills to disk)
    df_event.persist(StorageLevel.MEMORY_AND_DISK)

    # Call aggregation function
    aggregation_df = data_processor.aggregation(df_event, partition_cols, order_col, aggregation_cols)
    aggregation_df.show(truncate=False)

    # Write partitioned data using parquet format
    try:
        aggregation_df.write \
            .mode("append") \
            .partitionBy("id") \
            .parquet(output_path)
        
    except Exception as e:
        print(str(e))