import argparse
import logging
import re
from datetime import datetime
import pytz

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import coders


def parse_timestamp(str_ts):
    """
    Convert Common Log time format into a Python datetime object in UTC
    """
    local_ts = datetime.strptime(str_ts, '%d/%b/%Y:%H:%M:%S %z')
    return local_ts.astimezone(pytz.utc).isoformat()


def str_to_int(str_num):
    try:
        return int(str_num)
    except Exception as e:
        return 0


class ParseAndFilterDoFn(beam.DoFn):
    def process(self, element):
        pattern = rb'(^\S+?)\s.+?\[(.+)\]\s+"(.+?)\s(\S+)\s*(.*)"\s(\d+)\s(.+)'
        match = re.match(pattern, element)
        if match is not None:
            res = {}
            res['host'] = match.group(1).decode('utf-8')
            res['utc_timestamp'] = parse_timestamp(match.group(2).decode('utf-8'))
            res['action'] = match.group(3).decode('utf-8')
            res['uri'] = match.group(4).decode('utf-8')
            res['protocol'] = match.group(5).decode('utf-8')
            res['status'] = match.group(6).decode('utf-8')
            res['size'] = str_to_int(match.group(7).decode('utf-8'))
            yield res
        else:
            return


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
    parser.add_argument('--input_subscription',
                        dest='input_subscription',
                        help=('Input PubSub subscription of the form '
                              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DirectRunner',
        '--job_name=parse_nasa_log',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Specification for table in BigQuery
    table_spec = 'jf-project-20190218:nasa_logs.raw_access_logs'
    table_schema = 'host:STRING, utc_timestamp:TIMESTAMP, action:STRING, uri:STRING, protocol:STRING, status:STRING, size:INTEGER'
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        if known_args.input_subscription:
            lines = (
                p 
                | ReadFromPubSub(
                    subscription=known_args.input_subscription)
                .with_output_types(bytes))
        else:
            lines = (
                p 
                | ReadFromText(known_args.input, coder=coders.BytesCoder()))

        output = (
            lines 
            | 'parse_filter' >> beam.ParDo(ParseAndFilterDoFn()))
            # | 'parse' >> (beam.Map(parse_one_record)))

        # output | WriteToText(known_args.output)
        output | WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()