import luigi
import json
import os
import re
import natsort
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dataFrameTask import DataFrameTask
from configDatasetTask import ConfigDatasetTask

# Compute basic statistics for every variable in the dataset
# TODO: should read from a DB
class DescriptiveStatsTask( luigi.Task ):
    reportID = luigi.Parameter()

    def requires( self ):
        return {
            'data': DataFrameTask(self.reportID),
            'config': ConfigDatasetTask(self.reportID),
        }


    def run( self ):

        # load CSV configuration file
        configFilename = self.input()['config'].fn
        config = json.load( open(configFilename, 'r') )
        dependentVariables = config['dependentVariables']

        # output image format
        self.saveFigFormat = config['saveFigFormat']

        # prepare folders in which plots will be saved
        self.prepareOutputDirectories()

        # load data from pickle
        dataFrameFilename = self.input()['data'].fn
        self.df = pd.read_pickle( dataFrameFilename )
        print self.df.dtypes

        # compute stats and plots for all columns
        self.stats = {}
        for col in self.df:

            # univariate
            self.statsForOneVariable( col )

            # bivariate (called only for independent variables)
            for dependentVariable in dependentVariables:
                if ( col != dependentVariable ):
                    self.statsForTwoVariables( col, dependentVariable )


        out = self.output().open('w')
        out.write( json.dumps( self.stats, indent=4 ) )
        out.close()

    def statsForOneVariable( self, col ):

        self.stats[col] = self.df[col].describe().to_dict()

        if self.stats[col]['count'] > 0:

            if ( self.df[col].dtype == 'object' ):
                # nominal values
                self.statsForOneVariableNominal( col )

            else:
                # TODO: don't throw in here the dates, only numbers
                self.statsForOneVariableNumerical( col )


    def statsForOneVariableNominal(self, col):

        maxCats = 35  # don't want too many categories in the report

        # count values in each category and sort by frequency, useful if we have too many categories
        valuesCount = self.df[col].value_counts( dropna=False ).sort_values( ascending=False )
        maxValuesCount = valuesCount[:maxCats]

        counts = maxValuesCount.values
        levels = maxValuesCount.index.values

        # bar plot
        plt.figure() # reset matplotlib otherwise plots are overridden
        bar_order = self.deduceBarOrder(levels)
        plot = sns.barplot( y=levels, x=counts, orient='h', color='#557799', order=bar_order )
        plt.subplots_adjust(left=0.3, right=0.9, top=0.9, bottom=0.1)
        escapedColName = self.escapeColumnName(col)
        plt.title(escapedColName)
        plot.figure.savefig(self.distributionsDir + escapedColName + '.' + self.saveFigFormat)

        # stats
        self.stats[col]['value_counts'] = maxValuesCount.to_dict()  #only the most frequent

    def statsForOneVariableNumerical(self, col):

        notNaNValues = self.df[col].dropna().values
        nBins = 10

        # distribution plot
        plt.figure() # reset matplotlib otherwise plots are overridden
        plot = sns.distplot( notNaNValues, kde=False )
        escapedColName = self.escapeColumnName(col)
        plt.title(escapedColName)
        plot.figure.savefig(self.distributionsDir + escapedColName + '.' + self.saveFigFormat)

        # histogram for stast
        hist, bins = np.histogram( notNaNValues, bins=nBins )
        self.stats[col]['histogram'] = {
            'hist': hist.tolist(),
            'bins': bins.tolist()
        }

    def statsForTwoVariables(self, x, y):

        print '\n going to plot:', x, 'vs', y, ', with types:',  self.df[x].dtype,  self.df[y].dtype
        # TODO: consider stats.unique() to optimize chart beauty (put horizontally the variable with most levels)

        xType = self.df[x].dtype
        yType = self.df[y].dtype

        plt.figure()

        if( xType == 'object' and yType == 'object'):   # both nominal
            # grouped bar chart
            hue_order = self.deduceBarOrder(self.df[y].unique())
            bar_order = self.deduceBarOrder(self.df[x].unique())
            plot = sns.countplot( x=x, hue=y, data=self.df, order=bar_order, hue_order=hue_order )
            plt.subplots_adjust(left=0.2, right=0.9, top=0.9, bottom=0.3)
            plt.xticks(rotation=60)
            plot = plot.figure

        elif( xType == 'object' or yType == 'object' ): # one is nominal

            # nominal goes on the y
            x_axis = x
            y_axis = y
            if( yType == 'object' ):
                tmp = x
                x_axis = y
                y_axis = tmp

            # box plot
            bar_order = self.deduceBarOrder(self.df[x_axis].unique())
            plot = sns.boxplot( x=y_axis, y=x_axis, data=self.df, orient='h', sym='', order=bar_order )        # no outliers
            plt.subplots_adjust(left=0.4, right=0.9, top=0.9, bottom=0.3)
            plot = plot.figure

        else:                                           # both on scale
            # kde/hexbin!!!
            plot = sns.jointplot( x=x, y=y, data=self.df, kind='kde' )  # eventually kind='hex'

        # save chart as image
        escapedColName = self.escapeColumnName( y + '_vs_' + x )
        plt.title(escapedColName)
        plot.savefig(self.relationsDir + escapedColName + '.' + self.saveFigFormat)


    def escapeColumnName(self, name):
        return name.replace("/", "")

    def prepareOutputDirectories(self):
        # set output directories
        self.distributionsDir = 'data/tmp/distributions/'
        self.relationsDir     = 'data/tmp/relations/'

        # ensure directories exist
        for outD in [self.distributionsDir, self.relationsDir]:
            if not os.path.exists(outD):
                os.makedirs(outD)

    def deduceBarOrder(self, labels):
        # check if labels contain numbers
        labels_contain_numbers = False
        for label in labels:
            if not pd.isnull(label):
                labels_contain_numbers = labels_contain_numbers or bool(re.search(r'\d', label))

        if labels_contain_numbers:
            return natsort.natsorted(labels)

        # default is: no order
        return None

    def output( self ):

        fileName = self.reportID.split('/')
        fileName = fileName[-1]
        return luigi.LocalTarget( "data/tmp/" + fileName + ".descriptiveStats.json" )