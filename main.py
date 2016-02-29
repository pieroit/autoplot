
import luigi
import os
import glob
import urllib
from pprint import pprint
from woocommerce import API
from reportTask import ReportTask

def cleanFolder(regex):

    files2remove = glob.glob(regex)
    for f in files2remove:
        os.remove(f)

def getOrders():

    print "asking stuff to WC"
    wc = API(
        url="http://localhost/woocommerce",
        consumer_key="ck_39b4df847641164ccba1995a498c954997a6eab8",
        consumer_secret="cs_1f018f0f4e46dab7572d68eae63941d0b0adac63",
        version="v3"
    )
    print wc

    res = wc.get('orders/?filter[meta]=true').json()
    orders = res["orders"]
    inputFiles = []
    for order in orders:
        pprint(order)
        if 'order_meta' in order:
            if 'order_dataset_link' in order['order_meta']:
                link = order['order_meta']['order_dataset_link']
                urllib.URLopener().retrieve( link, "data/in/" + str(order['id']) )
                inputFiles.append( str(order['id']) )
        print ""

    return inputFiles


if __name__ == "__main__":

    # Remove produced file for debugging purposes
    cleanFolder('data/tmp/*')
    cleanFolder('data/out/*')

    # Get latest order from wooCommerce
    orders = getOrders()
    print orders[0]
    # Launch pipeline
    luigi.run( ["--local-scheduler", "--reportID", orders[0]], main_task_cls=ReportTask )