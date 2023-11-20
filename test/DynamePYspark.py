# generate_pyspark_script.py

def generate_script(data_path, cleaning_rules):
    # Load JSON rules
    rules = json.loads(cleaning_rules)

    # Initialize Spark session
    spark = SparkSession.builder.appName('DataCleaning').getOrCreate()

    # Load data
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Apply cleaning rules
    for rule in rules:
        if rule['type'] == 'filter':
            df = df.filter(rule['condition'])
        elif rule['type'] == 'rename':
            df = df.withColumnRenamed(rule['old_column'], rule['new_column'])
        # Add more conditions based on the types of rules you want to support

    # Save cleaned data
    df.write.mode('overwrite').csv('cleaned_data')
