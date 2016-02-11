import luigi
import pandas as pd
from mergeDatasetsTask import MergeDatasetsTask

# Transform CSV in a typed DataFrame. Should read types from a configuration and guess the rest.
# TODO: should write to a typed DB
class DataFrameTask( luigi.Task ):

    def requires( self ):
        return MergeDatasetsTask()

    def run( self ):

        # Open CSV with pandas
        df = pd.read_csv( self.input().fn )
        df = df.convert_objects( convert_numeric=True )

        # Save as pickle
        df.to_pickle( self.output().fn )

    def output( self ):
        return luigi.LocalTarget( "data/dataFrame.pkl" )