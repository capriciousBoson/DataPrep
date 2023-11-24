from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.types import ShortType, FloatType 
from google.cloud import storage
import json
import sys

def read_json_from_gcs(bucket_name, file_path):
    # Create a storage client
    storage_client = storage.Client()

    # Get the bucket and blob
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Download the JSON data
    json_data = blob.download_as_text()

    # Parse the JSON data into a Python dictionary
    json_object = json.loads(json_data)

    return json_object

def mean_normalization(df, column_name):
    """
    Perform mean normalization on the specified column in the DataFrame.
    """
    print("column anme is :: --- ",column_name)
    mean_value = df.agg(functions.mean(col(column_name))).collect()[0][0]
    std_dev_value = df.agg(functions.stddev(col(column_name))).collect()[0][0]

    if std_dev_value ==0:
        return df
    print("Calculated vlaues of mean =",mean_value," and std deviation is" ,std_dev_value," ...........................................................")
    normalized_column = ((col(column_name) - mean_value) / std_dev_value).alias(column_name + "_normalized")
    df_normalized = df.select("*", normalized_column)

    return df_normalized

def process_data(input_path, output_path, operations):
    """
    Read a CSV file from the specified input path, perform operations based on the provided dictionary,
    and save the processed DataFrame to the specified output path.
    """
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
    # Read CSV file from Google Cloud Storage
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print(operations)
    # Perform specified operations
    for column_name, operation in operations.items():
        if operation == "mean_normalization":
            print("staring mean normalization...................")
            df = mean_normalization(df, column_name)
            print("finished mean normalization..............................")
        # Add more operations as needed

    # Write the processed DataFrame to Google Cloud Storage
    df.write.csv(output_path, header=True, mode="overwrite")

    spark.stop()
if __name__ == "__main__":
    # Check if the correct number of command-line arguments are provided
    """"
    if len(sys.argv) != 4:
        print("Usage: python script.py <input_path> <output_path> <operations_dict>")
        sys.exit(1)
    """

    input_path = sys.argv[1]
    output_path = "gs://dataprep-bucket-001/Processed-Data/processed_data_123467.csv"
    operations_dict_str = {"reads":"mean_normalization"}

    try:
        # Convert the operations string to a dictionary
        #operations = eval(operations_dict_str)

        # Call the main processing function
        process_data(input_path, output_path,operations_dict_str )
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

