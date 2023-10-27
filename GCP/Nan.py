import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

class RemoveNaNValues(beam.DoFn):
    def process(self, element):
        cleaned_data = [field.strip() for field in element.split(',') if field.strip() != 'NaN']
        if all(cleaned_data):
            yield ','.join(cleaned_data)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file path')
    parser.add_argument('--output', dest='output', required=True, help='Output file path')

    pipeline_options = PipelineOptions(argv)

    with beam.Pipeline(options=pipeline_options) as p:
        csv_data = p | 'ReadFromText' >> beam.io.ReadFromText(parser.parse_known_args(argv)[0].input)
        cleaned_data = csv_data | 'ProcessCSV' >> beam.ParDo(RemoveNaNValues())
        cleaned_data | 'WriteToText' >> beam.io.WriteToText(parser.parse_known_args(argv)[0].output)

if __name__ == '__main__':
    run()
