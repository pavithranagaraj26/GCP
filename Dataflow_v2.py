
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
                'description:STRING, title:STRING, formats:STRING, favorite:STRING, years:STRING, year:INTEGER')

class XmlParser(beam.DoFn):
    def process(self,x):
      root = ET.fromstring(x)
      mylist=[]
      myDict={}
      for child in root:
	     for schi in child:
	       for sschi in schi:
	         #print (child.tag,child.attrib['category'],schi.tag,schi.attrib['years'],sschi.tag,sschi.attrib['favorite'],sschi.attrib['title'])
	         for i in child.attrib:
	           myDict[i]=child.attrib[i]
	         for i in schi.attrib:
	           myDict[i]=schi.attrib[i]
	         for i in sschi.attrib:
	           myDict[i]=sschi.attrib[i]
	           myDict['formats']=sschi.find('format').text
	           myDict['year']=sschi.find('year').text
	           myDict['rating']=sschi.find('rating').text
	           myDict['description']=sschi.find('description').text
	           mylist.extend([myDict.copy()])
	           print ">>>>",mylist,"\n"
      #print mylist
      return mylist

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
                      default='gs://gcp-practice-218808.appspot.com/complexDataset.xml',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      default='gs://gcp-practice-218808.appspot.com/test',
                      help='Output file to write results to.')
  parser.add_argument(
      '--output_table', required=False,
      help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
      'or DATASET.TABLE.'))
  parser.add_argument(
      '--runner', required=False,
      default='DataflowRunner',
      help=('which environemnt to run'))

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)


  pcoll = (p
           | 'read' >> ReadFromText(known_args.input) # Read the text file[pattern] into a PCollection.
           #| 'read' >> beam.io.Read(known_args.input)
           #| 'processing' >> beam.ParDo(ComputeFunction()) #pardo will be used to run custom function
           | 'parsing' >> beam.ParDo(XmlParser())
           #| 'Format' >> beam.ParDo(FormatDoFn())
           #| 'output' >> WriteToText(known_args.output)
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