import luigi
import json
import pandas as pd

class ConfigDatasetTask( luigi.Task ):
    reportID = luigi.Parameter()

    def run(self):

        inputFile = self.reportID
        self.previewFile( inputFile )

        # TODO: checkout easygui
        separator = raw_input("Separator: ") or ","    # TODO .split() heuristic will do
        decimal   = raw_input("Decimal: ") or "."
        thousands = raw_input("Thousands: ") or None
        encoding  = raw_input("Encoding: ") or "utf-8"

        # parse first few lines
        csvHead = pd.read_csv( inputFile, sep=separator, decimal=decimal, nrows=100 )

        # TODO: deal with encoding: https://pypi.python.org/pypi/chardet OR
        # TODO: dealing with NaN
        # http://www.datacarpentry.org/python-ecology/03-data-types-and-format
        config = {
            'usecols': [],
            'dtypes': {},
            'separator': separator,
            'decimal': decimal,
            'thousands': thousands,
            'encoding': encoding
        }
        for c in csvHead:
            print csvHead[c]
            # http://docs.scipy.org/doc/numpy-1.10.1/user/basics.types.html
            print 'Choose among [ object, float64, int64, datetime64[ns] ]' # TODO: deal with motherfucking dates
            assignedType = raw_input(c + " dtype: ") or None
            if assignedType:
                config['usecols'].append(c)
                config['dtypes'][c] = assignedType


        # save config file
        out = self.output().open('w')
        out.write( json.dumps( config, indent=4 ) )
        out.close()

    def previewFile( self, fileName):

        print('==========' + fileName + '===========\n')
        file = open(fileName, 'r')
        for i in range(10):
            print file.readline()
        file.close()
        print('============================\n')

    def output( self ):
        fileName = self.reportID.split('/')
        fileName = fileName[-1]
        return luigi.LocalTarget( "data/in/" + fileName + ".config.json" )
