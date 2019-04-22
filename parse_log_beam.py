from __future__ import print_function
from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import coders


def parse_timestamp():
    """
    Convert Common Log time format into a Python datetime object
    """
    pass

class ParseAndFilterDoFn(beam.DoFn):
    def process(self, element):
        pattern = rb'(^\S+?)\s.+?\[(.+)\]\s+"(.+?)\s(\S+)\s*(.*)"\s(\d+)\s(.+)'
        match = re.match(pattern, element)
        if match is not None:
            yield list(match.groups())
        else:
            return


def parse_one_record(one_rec):
    """
    Parse one row of log into fields according to Common Logfile Format
    """
    pattern = rb'(^\S+?)\s.+?\[(.+)\]\s+"(.+?)\s(\S+)\s*(.*)"\s(\d+)\s(.+)'
    line = one_rec
    match = re.match(pattern, line)
    return list(match.groups())

def run(argv=None):
    """
    Main entry point, define and run the pipeline
    """
    parser = argparse.ArgumentParser(description='Run Apache Beam to process the logs')
    parser.add_argument('--input',
                        dest='input',
                        help='Input file to process')
    parser.add_argument('--output',
                        dest='output',
                        help='Output file to write results to')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DirectRunner',
        '--job_name=parse_nasa_log',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=coders.BytesCoder())

        output = (
            lines 
            | 'parse_filter' >> beam.ParDo(ParseAndFilterDoFn()))
            # | 'parse' >> (beam.Map(parse_one_record)))

        output | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
