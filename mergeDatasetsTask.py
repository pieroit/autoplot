import luigi

# Merge two CSV
class MergeDatasetsTask( luigi.Task ):

    def output( self ):
        return luigi.LocalTarget( "data/cordis-h2020projects.csv" )