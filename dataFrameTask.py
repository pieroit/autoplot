from libxml2mod import xmlTextReaderQuoteChar
import luigi
import json
import pandas as pd
from mergeDatasetsTask import MergeDatasetsTask
from configDatasetTask import ConfigDatasetTask

# Transform CSV in a typed DataFrame. Should read types from a configuration and guess the rest.
# TODO: should write to a typed DB
class DataFrameTask( luigi.Task ):
    reportID = luigi.Parameter()

    def requires( self ):
        return {
            'data': MergeDatasetsTask(self.reportID),
            'config': ConfigDatasetTask(self.reportID)
        }

    def run( self ):

        # load CSV filename and relative JSON configuration file
        csv = self.input()['data'].fn
        config = json.load( self.input()['config'].open('r') )

        # Open CSV with pandas
        #df = pd.read_csv( self.input().fn, sep=";", dtype=types )
        #df = pd.read_csv( self.input().fn, sep=";", dtype=types, encoding='latin-1' )
        df = pd.read_csv( csv, sep=config['separator'], decimal=config['decimal'], dtype=config['dtypes'] )

        # Save as pickle
        df.to_pickle( self.output().fn )

    def output( self ):
        return luigi.LocalTarget( "data/tmp/" + self.reportID + ".dataFrame.pkl" )