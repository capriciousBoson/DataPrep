
import re
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
    # print(f"Output Path: {output_path}"

    # Use the split method to split the string at the first underscore
    userID= input_path.split('_')[0]
    # Print the result
    print(userID)
    # Set the GCS bucket path to your JSON files
    tranformationConfig = f"gs://your-bucket-name/path/to/json/files/{userID}_configuration.json"
    # Read JSON data from GCS
    rule_data = spark.read.json(tranformationConfig)
    # Show the DataFrame
    rule_data.show()


    # Read the CSV file into a DataFrame
    # df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = spark.read.option("sep", "\t").csv(data_path, header=True, inferSchema=True)


    
    # add code to process file dynamicaly fased on config 
    # sample below 
    # for rule in rule_data:
    #     if rule['type'] == 'filter':
    #         df = df.filter(rule['condition'])
    #     elif rule['type'] == 'rename':
    #         df = df.withColumnRenamed(rule['old_column'], rule['new_column'])
    #     # Add more conditions based on the types of rules you want to support

    # Clean data
    columns_to_keep = ["sequence_id", "sequence", "experiment_type", "dataset_name", "reads", "signal_to_noise", "SN_filter"]
    cleaned_df = df.select(columns_to_keep)

    # Write cleaned data to a storage location
    output_path = "path_to_your_output_data"
    cleaned_df.write.option("sep", "\t").csv(output_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()