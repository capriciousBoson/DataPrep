from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("CSVProcessing").getOrCreate()

    # Get input and output paths from command line arguments
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Read the CSV file into a DataFrame
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Perform a basic transformation: square the values in the 'value' column
    df_transformed = df.withColumn("value_squared", col("value") ** 2)

    # Write the transformed DataFrame back to a CSV file
    df_transformed.write.csv(output_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()