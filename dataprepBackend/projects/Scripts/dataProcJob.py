from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.types import ShortType, FloatType 
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from google.cloud import storage
import json
import sys

def mean_normalization(df, column_name):
    """
    Perform mean normalization on the specified column in the DataFrame.
    """
    mean_value = df.agg(functions.mean(col(column_name))).collect()[0][0]
    std_dev_value = df.agg(functions.stddev(col(column_name))).collect()[0][0]

    # Avoid division by zero
    if std_dev_value == 0:
        return df

    normalized_column = ((col(column_name) - mean_value) / std_dev_value).alias(column_name + "_normalized")
    df_normalized = df.select("*", normalized_column)

    return df_normalized

def categorical_encoding(df, col_name):
    print(f"starting categorical encoding for {col_name} ...................")
    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index")
    df = indexer.fit(df).transform(df)
    print(f"Finished categorical encoding for {col_name} ...................")
    return df

def fillNaNumerical(df, column_name):
    print(f"starting Fill NaN  for {col_name} ...................")
    mean_value = df.agg({column_name: 'mean'}).collect()[0][0]
    df = df.fillna(mean_value, subset=col)
    print(f" Filled Nan for {col_name} ...................")

def smartProcess(df):
    # Separate columns into numerical and string columns
    print("Automatic processing of the data  started.... ")
    schema = df.schema
    
    numerical_columns = [field.name for field in schema.fields if 'StringType' not in str(field.dataType)]
    string_columns = [field.name for field in schema.fields if 'StringType' in str(field.dataType)]

    print(f" Numerical Columns detected : {numerical_columns} \n String Columns detected : {string_columns}")

    # Handling Missing Values for Numerical Columns
    for col_name in numerical_columns:
        mean_value = df.agg({col_name: 'mean'}).collect()[0][0]
        df = df.fillna(mean_value, subset=col_name)

    # Handling Missing Values for String Columns
    for col_name in string_columns:
        df = df.fillna("UNKNOWN", subset=col_name)

    # Calculate the average length of strings and cardinality in each string column
    for col_name in string_columns:
        avg_length = df.agg(avg(length(col(col_name))).alias('avg_length')).collect()[0]['avg_length']
        cardinality = df.agg(countDistinct(col(col_name)).alias('cardinality')).collect()[0]['cardinality']
        
        print(f"Calculated Avg length for column - {col_name} = {avg_length}")
        print(f"Calculated Cardinality for column - {col_name} = {cardinality}")

        # Define thresholds based on your criteria
        short_text_threshold = 50  # for example, strings with 50 characters or less are considered short
        long_text_threshold = 100  # for example, strings with 100 characters or more are considered long
        cardinality_threshold_percentage = 0.9  # for example, 90% of the total number of rows

    # Decide whether to treat the string column as categorical based on average length and cardinality
    if avg_length <= short_text_threshold and cardinality <= cardinality_threshold_percentage * df.count():
        print(f"The strings in '{col_name}' are likely short and have low cardinality, possibly suitable for categorical encoding.")
        # Perform categorical encoding using StringIndexer
        df = categorical_encoding(df, col_name)

    elif short_text_threshold < avg_length <= long_text_threshold:
        print(f"The strings in '{col_name}' are of moderate length.")
    else:
        print(f"The strings in '{col_name}' are likely long, possibly requiring additional text processing.")

    for col_name in numerical_columns:
        df = fillNaNumerical(df, col_name)
        df = mean_normalization(df, col_name)

    return df



def process_data(input_path, output_path, operations):
    """
    Read a CSV file from the specified input path, perform operations based on the provided dictionary,
    and save the processed DataFrame to the specified output path.
    """
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    # Read CSV file from Google Cloud Storage
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Perform specified operations
    for column_name, operation in operations.items():
        if operation == "smart_process":
            df == smartProcess(df)
        if operation == "mean_normalization":
            df = mean_normalization(df, column_name)
        # Add more operations as needed

    # Write the processed DataFrame to Google Cloud Storage
    #df.write.csv(output_path, header=True, mode="overwrite")


    df.coalesce(1).write.csv(output_path , header=True, mode="overwrite")


    spark.stop()

def read_json_from_gcs(gcs_path):
    # Create a GCS client
    client = storage.Client()

    # Parse the GCS path
    bucket_name, blob_name = gcs_path.split('gs://')[1].split('/', 1)

    # Get the bucket and blob
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the JSON content from GCS
    json_content = blob.download_as_text()

    # Convert the JSON content to a Python dictionary
    json_dict = json.loads(json_content)

    return json_dict


if __name__ == "__main__":
    # Check if the correct number of command-line arguments are provided
    #if len(sys.argv) != 4:
    #    print("Usage: python script.py <input_path> <output_path> <operations_dict>")
    #    sys.exit(1)

    #input_path = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
    print("sys.argv...........................",sys.argv,sys.argv[2])
    input_path = sys.argv[2]
    print("input_path................................",input_path, type(input_path))
    path_parts = input_path.split('/')

    # Extract the desired part
    username = path_parts[-2]

    dataset_name = path_parts[-1][:-4]

    print("username = ", username, " dataset name = ", dataset_name)
    #operations_dict_str = sys.argv[3]
    output_path = f"gs://dataprep-bucket-001/Processed-Data/{username}/{dataset_name}_processed_data"
    print ("output path= ", output_path)
    #output_path = sys.argv[3]
    #operations = {"reads":"mean_normalization"}
    operations_json_path = f'gs://dataprep-bucket-001/Raw-Data/{username}/{dataset_name}.json'
    print ("config_osn path= ", operations_json_path)
    operations = read_json_from_gcs(operations_json_path)

    #operations = sys.argv[4]
    print("operations-----",operations,type(operations))

    try:
        # Convert the operations string to a dictionary
        #operations = eval(operations_dict_str)

        # Call the main processing function
        process_data(input_path, output_path, operations)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


