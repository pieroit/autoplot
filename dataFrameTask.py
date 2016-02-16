from libxml2mod import xmlTextReaderQuoteChar
import luigi
import pandas as pd
from mergeDatasetsTask import MergeDatasetsTask

# Transform CSV in a typed DataFrame. Should read types from a configuration and guess the rest.
# TODO: should write to a typed DB
class DataFrameTask( luigi.Task ):

    def requires( self ):
        return MergeDatasetsTask()

    def run( self ):

        # Variable types
        # http://docs.scipy.org/doc/numpy-1.10.1/user/basics.types.html
        # one among: 'int64', 'float64', 'uint64', 'datetime64[ns]', 'timedelta64[ns]', 'complex128', 'object', 'bool'
        # TODO: load externally + UI to produce it
        types = {
            'rcn'                 :'object',
            'reference'           :'object',
            # TODO: deal with motherfucking dates
            #'startDate'           :'datetime64[s]',
            #'endDate'             :'datetime64[s]',
            'totalCost'           :'float64',
            'ecMaxContribution'   :'float64',
            'subjects'              :'float64'
        }

        # TODO: how do I know the separator?
        # TODO: dela with encoding: https://pypi.python.org/pypi/chardet OR
        # TODO: deal with decimal and thousands - . OR ,
        # TODO: dealing with NaN
        # http://www.datacarpentry.org/python-ecology/03-data-types-and-format

        # Open CSV with pandas
        #df = pd.read_csv( self.input().fn, sep=";", dtype=types )
        #df = pd.read_csv( self.input().fn, sep=";", dtype=types, encoding='latin-1' )
        df = pd.read_csv( self.input().fn, sep=";", decimal=",", dtype=types )

        # Save as pickle
        df.to_pickle( self.output().fn )

    def output( self ):
        return luigi.LocalTarget( "data/dataFrame.pkl" )