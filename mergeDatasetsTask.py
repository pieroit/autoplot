import luigi

# Merge two CSV
class MergeDatasetsTask( luigi.Task ):

    def output( self ):
        return luigi.LocalTarget( "data/fornitori.csv" )