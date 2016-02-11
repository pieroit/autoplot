import luigi
from descriptiveStatsTask import DescriptiveStatsTask

# Compose data for the HTML5 report
class ReportTask( luigi.Task ):

    def requires( self ):
        return DescriptiveStatsTask()

    def run( self ):
        out = self.output().open('w')
        out.write( "ST1" )
        out.close()

    def output( self ):
        return luigi.LocalTarget( "data/stocazzo.txt" )