import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.dataframe.io import read_csv, to_csv

import logging
import argparse



options = PipelineOptions()
p = beam.Pipeline(options=options)



def print_row(element):
  print(element)



## class with input and output files
class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input-file',
        default='iris.csv',
        help='The file path for the input text to process.')
    parser.add_argument(
        '--output-path',
        default='out.csv',
        help='The path prefix for output files.')




class Split(beam.DoFn):
    """split to columns and select 2 cols"""
    def process(self, element):
            sepallength,sepalwidth,petallength,petalwidth,variety = element.split(",")
            return [{
                'sepallength':float(sepallength),
                'sepalwidth':float(sepalwidth),
                'petallength':float(petallength),
                'petalwidth':float(petalwidth),
                'variety':variety,  # these arrive out of order as theyre an unordered set for each row
            }]


def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line





def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    df =  p | read_csv(known_args.input) 
        #beam.ParDo(Split()) |
        #beam.Map(print_row) |   # returned results aren't great when this runs
    df['new'] = df['petal.length'] * df['sepal.width']
    df = df.query('new > 10')
    #print('\n\n\n\n\n\n\n\n\n')
    print(type(df))  # tells you df is a DeferredDataframe

    df2 = df.groupby(df.variety).describe()

    df2.to_csv(known_args.output, index=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()





"""
csv_lines = (p | 
    'ReadMyFile' >> beam.io.ReadFromText('/Users/adambricknell/Desktop/beam_test/iris.csv', skip_header_lines=1) |  
    'Print output' >> beam.Map(print_row) |  
    beam.ParDo(Split()) | 
    'WriteToText' >>  beam.io.WriteToText('/Users/adambricknell/Desktop/beam_test/out.csv')
    )
"""




