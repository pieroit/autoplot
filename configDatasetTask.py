import luigi
import json

# Merge two CSV
class ConfigDatasetTask( luigi.Task ):
    reportID = luigi.Parameter()

    def run(self):

        # Variable types
        # http://docs.scipy.org/doc/numpy-1.10.1/user/basics.types.html
        # one among: 'int64', 'float64', 'uint64', 'datetime64[ns]', 'timedelta64[ns]', 'complex128', 'object', 'bool'
        # TODO: load externally + UI to produce it
        types = {
            'rcn'                 :'object',
            'reference'           :'object',
            # TODO: deal with motherfucking dates
            #'startDate'          :'datetime64[s]',
            #'endDate'            :'datetime64[s]',
            'totalCost'           :'float64',
            'ecMaxContribution'   :'float64',
            'subjects'            :'float64'
        }

        # TODO: how do I know the separator?
        # TODO: dela with encoding: https://pypi.python.org/pypi/chardet OR
        # TODO: deal with decimal and thousands - . OR ,
        # TODO: dealing with NaN
        # http://www.datacarpentry.org/python-ecology/03-data-types-and-format

        config = {
            'dtypes': types,
            'separator': ';',
            'decimal': ',',
            'encoding': 'something'
        }

        out = self.output().open('w')
        out.write( json.dumps( config ) )
        out.close()

    def output( self ):
        return luigi.LocalTarget( "data/tmp/" + self.reportID + ".config.json" )