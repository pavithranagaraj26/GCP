
from __future__ import absolute_import

import argparse
import logging
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from xml.etree import ElementTree as ET

global T_SCHEMA;
global mySet;#set defined to hold xml tags and attributes uniquely
mySet=set()
"""
Reading Schema from a sample file provided through argument. File is placed in GCS
"""
def read_schema_from_bucket(filepath):
    import gcsfs
    fs = gcsfs.GCSFileSystem()
    with fs.open(filepath) as f:
       root=ET.fromstring(f.read())
       for i in root.iter():
             if len(i.attrib)>=1:
                for k,v in i.attrib.items():
                    mySet.add(k)
             else: 
                mySet.add(i.tag)
       T_SCHEMA = [i + ":STRING" for i in mySet]
       T_SCHEMA = ",".join(T_SCHEMA)
       print "the schema is ",T_SCHEMA
    print mySet
    return T_SCHEMA

"""
This class is used to Parse the xml file
"""
class XmlParser(beam.DoFn):
    def process(self,x):
      root = ET.fromstring(x)
      myList=[]
      def crawl(root, memo={},parent=''):
        if len(root.getchildren()) == 0:
            if parent[-1:][0].tag==root.tag:
              myList.append(memo.copy())
            if len(root.attrib)>=1:
              for k,v in root.attrib.items():
                 memo[k]=v
            else:
               memo[root.tag]=root.text
        else:
          if len(root.attrib)>=1:
            for k,v in root.attrib.items():
               memo[k]=v
          else:
            memo[root.tag]=str(9999)
          for child in root.getchildren():
            crawl(child, memo, root)
        return memo
      myData=crawl(root)
      return myList

def run(argv=None):
  """Main entry point; defines and runs the dataflow pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://gcp-practice-218808.appspot.com/complexDataset.xml',
                      help='Input files to process.')
  parser.add_argument('--schema',
                      dest='schema', required=True,
                      help='Path to sample file to extract schema from it')                      
  parser.add_argument(
      '--output_table', required=True,
      help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
    # CHANGE 1/4: (OPTIONAL) Change this to DataflowRunner to
    # run your pipeline on the Google Cloud Dataflow Service.
    '--runner=DataflowRunner',
    # CHANGE 2/4: Your project ID is required in order to run your pipeline on
    # the Google Cloud Dataflow Service.
    '--project=gcp-practice-218808',
    # CHANGE 3/4: Your Google Cloud Storage path is required for staging local
    # files.
    '--staging_location=gs://gcp-practice-218808.appspot.com/dataflow/staging/',
    # CHANGE 4/4: Your Google Cloud Storage path is required for temporary
    # files.
    '--temp_location=gs://gcp-practice-218808.appspot.com/dataflow/tmp/',
    '--job_name=my-job',
  ])
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)
  pcoll = (p
           # Read the XML file into a PCollection.
           | 'Read from GCS' >> ReadFromText(known_args.input)
           #Parsing the xml input
           | 'Parsing XML' >> beam.ParDo(XmlParser())
           #Write to BigQuery
           | 'Write to Big Query' >> beam.io.WriteToBigQuery(
               known_args.output_table,
               schema= read_schema_from_bucket(vars(known_args)['schema']),
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
               )
           )
  result = p.run()
  result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()