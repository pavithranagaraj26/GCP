
# coding: utf-8

# In[23]:


from __future__ import absolute_import

import argparse
import logging
import re

#from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from xml.etree import ElementTree as ET


# In[25]:


class ComputeFunction(beam.DoFn):
    def process(self,x):
        return x; # as of now nothing is being done and  mostly be used to make single line xml


TABLE_SCHEMA = ('category:STRING, rating:STRING, '
                'description:STRING, title:STRING, favorite:STRING, years:STRING, year:INTEGER, multiple:STRING,collection:STRING')

class XmlParser(beam.DoFn):
    def process(self,x):
      root = ET.fromstring(x)
      myList=[]
      def crawl(root, prefix='', memo={},parent=''):
        new_prefix = root.tag
        #if len(prefix) > 0:
        #   new_prefix = prefix + "_" + new_prefix
        if len(root.getchildren()) == 0:
            #memo[new_prefix] = root.text #if not diaplaying correctly fall to this
            if len(root.attrib)>=1:
              for k,v in root.attrib.items():
                 memo[k]=v
            else:
               memo[root.tag]=root.text
            if parent[-1:][0].tag==root.tag:
               myList.append(memo.copy())
        else:
          if len(root.attrib)>=1:
            for k,v in root.attrib.items():
               memo[k]=v
          else:
            memo[root.tag]=str(9999)
          for child in root.getchildren():
            crawl(child, new_prefix, memo, root)
        return memo
      myData=crawl(root);
      return myList

class FormatDoFn(beam.DoFn):
  def process(self, element):
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    data_m={'id': element[0],
             'first_name': element[1],
             'last_name':element[2],
             'email':element[3],
             'ip_address':element[4],
             'gender':element[5] }
    #print "my-data",[data_m]
    print data_m
    return [data_m]


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://assetexample/python/complexDataset.xml',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      default='gs://assetexample/python/test',
                      help='Output file to write results to.')
  parser.add_argument(
      '--output_table', required=False,
      help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
      'or DATASET.TABLE.'))
  parser.add_argument(
      '--job_name', required=True,
      help=('Job name of Dataflow'))
  

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
    # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
    # run your pipeline on the Google Cloud Dataflow Service.
    '--runner=DataflowRunner',
    # CHANGE 3/5: Your project ID is required in order to run your pipeline on
    # the Google Cloud Dataflow Service.
    '--project=red-delight-223804',
    # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
    # files.
    '--staging_location=gs://assetexample/dataflow/staging/',
    # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
    # files.
    '--temp_location=gs://assetexample/dataflow/tmp/',
  ])
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)


  pcoll = (p
           | 'read' >> ReadFromText(known_args.input) # Read the text file[pattern] into a PCollection.
           #| 'read' >> beam.io.Read(known_args.input)
           #| 'processing' >> beam.ParDo(ComputeFunction()) #pardo will be used to run custom function
           | 'parsing' >> beam.ParDo(XmlParser())
           #| 'Format' >> beam.ParDo(FormatDoFn())
           #| 'output' >> WriteToText(known_args.output) # used for writing to gcs
           )
           
  pcoll | 'Write' >> beam.io.WriteToBigQuery(
               known_args.output_table,
               schema=TABLE_SCHEMA,
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
               )
  result = p.run()
  result.wait_until_finish()




# In[26]:

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
