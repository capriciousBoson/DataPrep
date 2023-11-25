from pyspark.sql import SparkSession
import json
import sys

def apply_transformations(df, transformations):
    for transformation in transformations:
        if transformation['operation'] == 'filter':
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
    return df

def main(input_file, config_file):
    spark = SparkSession.builder.appName('DataTransformation').getOrCreate()

    df = spark.read.csv(input_file, header=True)

    with open(config_file, 'r') as f:
        config = json.load(f)

    df = apply_transformations(df, config['transformations'])

    df.write.csv('output.csv')

if __name__ == '__main__':
    input_file = sys.argv[1]
    config_file = sys.argv[2]
    main(input_file, config_file)


# ########################test json
# {
#   "transformations": [
#     {
#       "operation": "filter",
#       "column": "age",
#       "condition": "greater_than",
#       "value": "30"
#     },
#     {
#       "operation": "rename",
#       "old_name": "age",
#       "new_name": "Age"
#     },
#     {
#       "operation": "drop",
#       "column": "name"
#     },
#     {
#       "operation": "select",
#       "columns": ["Age"]
#     },
#     {
#       "operation": "fillna",
#       "column": "Age",
#       "value": "0"
#     },
#     {
#       "operation": "cast",
#       "column": "Age",
#       "type": "IntegerType"
#     },
#     {
#       "operation": "normalize",
#       "column": "Age",
#       "std_dev": 10
#     },
#     {
#       "operation": "dropna",
#       "column": "Age"
#     }
#   ]
# }
