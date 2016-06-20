from libxml2mod import xmlTextReaderQuoteChar
import luigi
import json
import pandas as pd
from configDatasetTask import ConfigDatasetTask

# Transform CSV in a typed DataFrame. Should read types from a configuration and guess the rest.
class DataFrameTask( luigi.Task ):
    reportID = luigi.Parameter()

    def requires( self ):
        return ConfigDatasetTask(self.reportID)

    def run( self ):

        # load CSV configuration file
        config = json.load( self.input().open('r') )

        # Open CSV with pandas
        df = pd.read_csv( self.reportID, sep=config['separator'], decimal=config['decimal'], thousands=config['thousands'], dtype=config['dtypes'], usecols=config['usecols'] , encoding=config['encoding'])

        # Save as pickle
        df.to_pickle( self.output().fn )

    def output( self ):
        fileName = self.reportID.split('/')
        fileName = fileName[-1]
        return luigi.LocalTarget( "data/tmp/" + fileName + ".dataFrame.pkl" )