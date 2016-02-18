import luigi

# Merge two CSV
class MergeDatasetsTask( luigi.Task ):
    reportID = luigi.Parameter()

    def output( self ):
        return luigi.LocalTarget( "data/in/" + self.reportID )