import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import config.py as con


def run_dataflow_pipeline(project_id, input_bucket, input_file, output_bucket, output_prefix):
    # Define the pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        temp_location=f'gs://{output_bucket}/temp',
        region='us-south1',  # Set your region
        save_main_session=True
    )

    # Create the pipeline
    p = beam.Pipeline(options=options)

    # Define the pipeline steps
    def process_element(element):
        # Assuming the data is in CSV format
        columns = element.split(',')

        # Replace NaN values in the third column with 0
        if len(columns) >= 3 and columns[2].strip().lower() == 'nan':
            columns[2] = '0'

        # Join the columns back into a CSV string
        updated_element = ','.join(columns)

        return updated_element

    # Read from Google Cloud Storage
    input_data = (
        p
        | 'ReadFromGCS' >> beam.io.ReadFromText(f'gs://{input_bucket}/{input_file}')
    )

    # Apply your custom transformation
    transformed_data = (
        input_data
        | 'Transform' >> beam.Map(process_element)
    )

    # Write back to Google Cloud Storage
    _ = (
        transformed_data
        | 'WriteToGCS' >> beam.io.WriteToText(f'gs://{output_bucket}/{output_prefix}')
    )

    # Run the pipeline
    result = p.run()
    result.wait_until_finish()



if __name__ == "__main__":
    # Replace these values with your actual values


    # Run the pipeline using the function
    run_dataflow_pipeline(con.project_id, con.input_bucket, con.input_file, con.output_bucket, con.output_prefix)
