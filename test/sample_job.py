from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

if __name__ == "__main__":
    print("PySpark script starting")
    
    # Initialize a Spark session
    spark = SparkSession.builder.appName("CSVProcessing").getOrCreate()

    # Get input and output paths from command line arguments
    input_path = sys.argv[1]
    # output_path = sys.argv[2]
    data_path=input_path
    print(f"Input Path: {input_path}")
    # print(f"Output Path: {output_path}")
    test_var=sys.argv[2]
    test_dict=sys.argv[3]
    print(test_var,test_dict)

    # Read the CSV file into a DataFrame
    # df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = spark.read.option("sep", "\t").csv(data_path, header=True, inferSchema=True)

    # Clean data
    columns_to_keep = ["sequence_id", "sequence", "experiment_type", "dataset_name", "reads", "signal_to_noise", "SN_filter"]
    cleaned_df = df.select(columns_to_keep)

    # Write cleaned data to a storage location
    output_path = "path_to_your_output_data"
    cleaned_df.write.option("sep", "\t").csv(output_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()
