from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
#from pyspark.sql.functions import col
#from pyspark.sql import functions
#from pyspark.sql.types import ShortType, FloatType 
from pyspark.ml.feature import StringIndexer
from google.cloud import storage
import json
import sys


BUCKET_NAME='inclasslab3'


def log_transform(df, numerical_columns):
    """
    Apply log transformation to specified numerical columns in a PySpark DataFrame.

    Parameters:
    - df: PySpark DataFrame
    - numerical_columns: List of numerical column names to be log-transformed

    Returns:
    - Transformed PySpark DataFrame
    """

    # Apply log transformation to specified columns
    for column in numerical_columns:
        # Check if the minimum value is greater than zero
        min_value = df.agg({column: "min"}).collect()[0][0]
        
        if min_value > 0:
            # Apply log transformation
            df = df.withColumn(column + '_log', F.log(F.col(column) + 1))
        else:
            print(f"Skipping log transformation for column '{column}' due to non-positive minimum value.")
            
    return df

def mean_normalization(df, column_name, mean_value):
    """
    Perform mean normalization on the specified column in the DataFrame.
    """
    #mean_value = df.agg(functions.mean(col(column_name))).collect()[0][0]
    std_dev_value = df.agg(F.stddev(F.col(column_name))).collect()[0][0]

    # Avoid division by zero
    if std_dev_value == 0:
        return df

    normalized_column = ((F.col(column_name) - mean_value) / std_dev_value).alias(column_name + "_normalized")
    df_normalized = df.select("*", normalized_column)

    return df_normalized

def categorical_encoding(df, column_name):
    print(f"starting categorical encoding for {column_name} ...................")
    indexer = StringIndexer(inputCol=column_name, outputCol=f"{column_name}_index")
    encoded_df = indexer.fit(df).transform(df)
    print(f"Finished categorical encoding for {column_name} ...................")
    return encoded_df

def fillNaNumerical(df, column_name,mean_value):
    print(f"starting Fill NaN  for {column_name} ...................")
    # mean_value = df.agg({col_name: 'mean'}).collect()[0][0]
    df = df.fillna(mean_value, subset=column_name)
    print(f" Filled Nan for {column_name} ...................")
    return df


def detectOutliers(df, numerical_columns):

    # Identifying the numerical columns in a spark dataframe
    #numeric_columns = [column[0] for column in df.dtypes if column[1]=='int']

    # Using the `for` loop to create new columns by identifying the outliers for each feature
    for column in numerical_columns:

        less_Q1 = 'less_Q1_{}'.format(column)
        more_Q3 = 'more_Q3_{}'.format(column)
        Q1 = 'Q1_{}'.format(column)
        Q3 = 'Q3_{}'.format(column)

        # Q1 : First Quartile ., Q3 : Third Quartile
        Q1 = df.approxQuantile(column,[0.25],relativeError=0)
        Q3 = df.approxQuantile(column,[0.75],relativeError=0)
        
        # IQR : Inter Quantile Range
        # We need to define the index [0], as Q1 & Q3 are a set of lists., to perform a mathematical operation
        # Q1 & Q3 are defined seperately so as to have a clear indication on First Quantile & 3rd Quantile
        IQR = Q3[0] - Q1[0]
        
        #selecting the data, with -1.5*IQR to + 1.5*IQR., where param = 1.5 default value
        less_Q1 =  Q1[0] - 1.5*IQR
        more_Q3 =  Q3[0] + 1.5*IQR
        
        isOutlierCol = 'is_outlier_{}'.format(column)
        
        df = df.withColumn(isOutlierCol,F.when((df[column] > more_Q3) | (df[column] < less_Q1), 1).otherwise(0))
    

    # Selecting the specific columns which we have added above, to check if there are any outliers
    selected_columns = [column for column in df.columns if column.startswith("is_outlier")]

    # Adding all the outlier columns into a new colum "total_outliers", to see the total number of outliers
    df = df.withColumn('total_outliers',sum(df[column] for column in selected_columns))

    # Dropping the extra columns created above, just to create nice dataframe., without extra columns
    df = df.drop(*[column for column in df.columns if column.startswith("is_outlier")])

    return df




def smartProcess(df):
    print("Automatic processing of the data started.... ")
    schema = df.schema

    numerical_columns = [field.name for field in schema.fields if 'StringType' not in str(field.dataType)]
    string_columns = [field.name for field in schema.fields if 'StringType' in str(field.dataType)]

    print(f"Numerical Columns detected: {numerical_columns} \n String Columns detected: {string_columns}")

    # Handling Missing Values for String Columns
    for column_name in string_columns:
        df = df.fillna("UNKNOWN", subset=column_name)

    # Calculate the average length of strings and cardinality in each string column
    for column_name in string_columns:
        col_stats = df.agg(
            F.avg(F.length(F.col(column_name))).alias('avg_length'),
            F.countDistinct(F.col(column_name)).alias('cardinality')
        ).collect()[0]

        avg_length = col_stats['avg_length']
        cardinality = col_stats['cardinality']

        print(f"Calculated Avg length for column - {column_name} = {avg_length}")
        print(f"Calculated Cardinality for column - {column_name} = {cardinality}")

        # Define thresholds based on your criteria
        short_text_threshold = 50
        long_text_threshold = 100
        cardinality_threshold_percentage = 0.9

        # Decide whether to treat the string column as categorical based on average length and cardinality
        if avg_length <= short_text_threshold and cardinality <= cardinality_threshold_percentage * df.count():

            print(f"The strings in '{column_name}' are likely short : {avg_length} and have low cardinality : {cardinality}, \
                   possibly suitable for categorical encoding.")
            # Perform categorical encoding using StringIndexer
            df = categorical_encoding(df, column_name)
        elif short_text_threshold < avg_length <= long_text_threshold:
            print(f"The strings in '{column_name}' are of moderate length.")
        else:
            print(f"The strings in '{column_name}' are likely long, possibly requiring additional text processing.")

    # Process numerical columns
    for column_name in numerical_columns:
        mean_value = df.agg({column_name: 'mean'}).collect()[0][0]
        df = fillNaNumerical(df, column_name,mean_value)

    df = detectOutliers(df, numerical_columns)

    for column_name in numerical_columns:    
        df = mean_normalization(df, column_name, mean_value)

    return df


def process_data(input_path, output_path, operations):

    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    # Read CSV file from Google Cloud Storage
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Perform specified operations
    for transformation in operations:
        if transformation['operation'] == "mean_normalization":
            mean_value = df.agg({transformation['column']: 'mean'}).collect()[0][0]
            df = mean_normalization(df, transformation['column'], mean_value)
        elif transformation['operation'] == 'filter':
            if transformation['condition'] == 'equals':
                df = df.filter(df[transformation['column']] == transformation['value'])
            elif transformation['condition'] == 'greater_than':
                df = df.filter(df[transformation['column']] > transformation['value'])
            elif transformation['condition'] == 'less_than':
                df = df.filter(df[transformation['column']] < transformation['value'])
        elif transformation['operation'] == 'rename':
            df = df.withColumnRenamed(transformation['old_name'], transformation['new_name'])
        elif transformation['operation'] == 'drop':
            df = df.drop(transformation['column'])
        elif transformation['operation'] == 'select':
            df = df.select(transformation['columns'])
        elif transformation['operation'] == 'fillna':
            df = df.fillna(transformation['value'], subset=[transformation['column']])
        elif transformation['operation'] == 'cast':
            df = df.withColumn(transformation['column'], df[transformation['column']].cast(transformation['type']))
        elif transformation['operation'] == 'auto':
            df = smartProcess(df)
        elif transformation['operation'] == 'categorical_encoding':
            df = categorical_encoding(df,transformation['column'])
        elif transformation['operation'] == 'detect_outliers':
            numerical_columns = [column[0] for column in df.dtypes if column[1]=='int']
            df = detectOutliers(df,transformation['column'], numerical_columns)
        elif transformation['operation'] == 'log_transformation':
            numerical_columns = [column[0] for column in df.dtypes if column[1]=='int']
            df = log_transform(df,transformation['column'], numerical_columns)

    # Write the processed DataFrame to Google Cloud Storage
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

    # Sample input_path = 'gs://dataprep-bucket-001/Raw-Data/subset_dataset.csv'
    print("sys.argv...........................",sys.argv,sys.argv[2])
    input_path = sys.argv[2]
    print("input_path................................",input_path, type(input_path))
    path_parts = input_path.split('/')

    # Extract the desired part
    username = path_parts[-2]

    dataset_name = path_parts[-1][:-4]

    print("username = ", username, " dataset name = ", dataset_name)
    output_path = f"gs://{BUCKET_NAME}/Processed-Data/{username}/{dataset_name}_processed_data"
    print ("output path= ", output_path)

    # sample operations = {"reads":"mean_normalization"}
    operations_json_path = f'gs://{BUCKET_NAME}/Raw-Data/{username}/{dataset_name}.json'
    print ("config_json path= ", operations_json_path)
    operations = read_json_from_gcs(operations_json_path)

    print("operations-----",operations,type(operations))

    try:
        process_data(input_path, output_path, operations)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
