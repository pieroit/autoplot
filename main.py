
import luigi
import os
from reportTask import ReportTask

def cleanPipeline( dir="" ):

    files2remove = [ "stocazzo.txt", "descriptiveStats.json", "dataFrame.pkl" ]

    for f in files2remove:
        fpath = dir + f

        if os.path.exists( fpath ):
            os.remove( fpath )

if __name__ == "__main__":

    # Remove produced file for debugging purposes
    cleanPipeline( "data/" )

    # Launch pipeline
    luigi.run( ["--local-scheduler"], main_task_cls=ReportTask )