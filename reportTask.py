import luigi
from descriptiveStatsTask import DescriptiveStatsTask

# Compose data for the HTML5 report
class ReportTask( luigi.Task ):
    reportID = luigi.Parameter()

    def requires( self ):
        return DescriptiveStatsTask(self.reportID)

    def run( self ):
        out = self.output().open('w')
        out.write( "ST1" )
        out.close()

    def output( self ):
        return luigi.LocalTarget( "data/out/" + self.reportID + ".txt" )