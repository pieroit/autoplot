
import luigi
import os
import shutil
import glob
import urllib
from pprint import pprint
from woocommerce import API
from reportTask import ReportTask

def cleanFolder(regex):

    files2remove = glob.glob(regex)
    for f in files2remove:
        if os.path.isdir(f):
            shutil.rmtree(f)    # remove directory
        else:
            os.remove(f)   # remove file



if __name__ == "__main__":

    # Remove produced file for debugging purposes
    cleanFolder('data/tmp/*')
    cleanFolder('data/out/*')

    datasetName = "/home/piero/Desktop/clienti/GreenPeace/donors/data/experience.csv"

    # Launch pipeline
    luigi.run( ["--local-scheduler", "--reportID", datasetName], main_task_cls=ReportTask )