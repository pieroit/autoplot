import luigi
import json
import pandas as pd
import numpy as np
from dataFrameTask import DataFrameTask

# Compute basic statistics for every variable in the dataset
# TODO: should read from a DB
class DescriptiveStatsTask( luigi.Task ):

    def requires( self ):
        return DataFrameTask()

    def run( self ):

        df = pd.read_pickle( self.input().fn )
        print df.dtypes

        stats = {}

        for col in df:
            stats[col] = df[col].describe().to_dict()

            if ( df[col].dtype == 'object' ):
                # nominal values
                if( float(stats[col]['unique']) / float(stats[col]['count']) < 0.8 ):
                    stats[col]['value_counts'] = df[col].value_counts().to_dict()
            else:
                # TODO: don't throw in here the dates, only numbers
                # numerical values (excluding NaN)
                hist, bins = np.histogram( df[col].dropna().values )    # TODO: smart binning
                stats[col]['histogram'] = {
                    'hist': hist.tolist(),
                    'bins': bins.tolist()
                }

            print col
            print stats[col]
            print ''

        print json.dumps(stats)

        out = self.output().open('w')
        out.write( json.dumps( stats ) )
        out.close()

    def output( self ):
        return luigi.LocalTarget( "data/descriptiveStats.json" )