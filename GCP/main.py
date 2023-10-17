import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import csv

def run(project, source_bucket, target_bucket):
    

    options = {
        'staging_location': 'gs://ieee-ompi-datasets/staging',
        'temp_location': 'gs://ieee-ompi-datasets/temp',
        'job_name': 'dataflow-crypto',
        'project': project,
        'max_num_workers': 24,
        'teardown_policy': 'TEARDOWN_ALWAYS',
        'no_save_main_session': True,
        'runner': 'DataflowRunner'
      }
    options = beam.pipeline.PipelineOptions(flags=[], **options)
    
    crypto_dataset = 'gs://{}/crypto-markets.csv'.format(source_bucket)
    processed_ds = 'gs://{}/transformed-crypto-bitcoin'.format(target_bucket)

    pipeline = beam.Pipeline(options=options)

    # 0:slug, 3:date, 5:open, 6:high, 7:low, 8:close
    rows = (
        pipeline |
            'Read from bucket' >> ReadFromText(crypto_dataset) |
            'Tokenize as csv columns' >> beam.Map(lambda line: next(csv.reader([line]))) |
            'Select columns' >> beam.Map(lambda fields: (fields[0], fields[3], fields[5], fields[6], fields[7], fields[8])) |
            'Filter bitcoin rows' >> beam.Filter(lambda row: row[0] == 'bitcoin')
        )
        
    combined = (
        rows |
            'Write to bucket' >> beam.Map(lambda (slug, date, open, high, low, close): '{},{},{},{},{},{}'.format(
                slug, date, open, high, low, close)) |
            WriteToText(
                file_path_prefix=processed_ds,
                file_name_suffix=".csv", num_shards=2,
                shard_name_template="-SS-of-NN",
                header='slug, date, open, high, low, close')
        )

    pipeline.run()

# execute transfomation
if __name__ == '__main__':
    print ('Run pipeline on the cloud')
    run(project='oceanic-sky-230504', source_bucket='ieee-ompi-datasets', target_bucket='ieee-ompi-datasets')