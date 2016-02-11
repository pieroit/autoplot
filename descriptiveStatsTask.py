import luigi
import json
import pandas as pd
from dataFrameTask import DataFrameTask

# Compute basic statistics for every variable in the dataset
# TODO: should read from a DB
class DescriptiveStatsTask( luigi.Task ):

    def requires( self ):
        return DataFrameTask()

    def run( self ):

        df = pd.read_pickle( self.input().fn )

        descriptiveStats = {}

        for col in df:
            # TODO: hereeee
            print df[col].value_counts()
            #print df[col].describe().to_json()
            descriptiveStats[col] = df[col].describe()
            #print json.dumps( df[col].describe() )

        print descriptiveStats

        stats = {
            "mean": 12.4,
            "variance": 0.5
        }

        out = self.output().open('w')
        out.write( json.dumps( stats ) )
        out.close()

    def output( self ):
        return luigi.LocalTarget( "data/descriptiveStats.json" )