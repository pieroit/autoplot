
import luigi
import os
import glob
from reportTask import ReportTask

def cleanFolder(regex):

    files2remove = glob.glob(regex)
    for f in files2remove:
        os.remove(f)

if __name__ == "__main__":

    # Remove produced file for debugging purposes
    cleanFolder('data/tmp/*')
    cleanFolder('data/out/*')

    # Launch pipeline
    reportID = 'cordis-h2020projects.csv'
    luigi.run( ["--local-scheduler", "--reportID", reportID], main_task_cls=ReportTask )