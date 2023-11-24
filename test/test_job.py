
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
    # 
    #  # Use the split method to split the string at the first underscore
    #    Split the string at the last occurrence of '/'
    
    parts = input_path.rsplit('/', 1)

    # Extract the part after the last '/'
    if len(parts) > 1:
        # Remove the file extension (assuming it's always ".csv")
        userID = parts[1].split('.')[0]
        print(userID)
    else:
        print("No '/' found in the input string pass corect USer ID format")
  
    # Print the result
    print(userID,"gs://main-5333/{userID}_configuration.json")
    # Set the GCS bucket path to your JSON files
    tranformationConfig = f"gs://main-5333/{userID}_configuration.json"
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