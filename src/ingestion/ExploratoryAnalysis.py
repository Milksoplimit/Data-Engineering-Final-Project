# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project.  Use pandas_profiling and exprt to <repo_root>\ingestion_analysis.

# Note - Some data sources may have multiple files that you will use.  There is  likely a primary dataset with the most of the data; target that primary dataset for this analysis.

# This file can be developed using Python Interactive in VS Code or as a straight script.

import sqlite3
import os
import pandas as pd
import pandas_profiling as pdp

# get path to current workspace to pull the secret key from the config file
here = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(here, 'IAKey.config')

# get the key
keyFile = open(filename)
key = keyFile.readlines()


#'''
#Get connection to db
path = key[4].strip()
connection = sqlite3.connect(path)
querey = connection.cursor()

#Get a single table
table = [a for a in querey.execute("SELECT * FROM matches")]
#Put table in a frame and pull only a few for sampling
dbframe = pd.DataFrame(table)
#dbframe = dbframe[0:25]
#Close the connection
querey.close()

#Get the csv file of players and trim it for sampling
path = "C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset2/players.csv"
csvframe = pd.read_csv(path, sep="|")
#csvframe = csvframe[0:25]

# make paths to output files
dataset1path = "C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/ingestion_analysis/DataSet1.csv"
dataset2path = "C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/ingestion_analysis/DataSet2.csv"
#'''

# make frames to hold results
set1frame = pd.DataFrame()
set2frame = pd.DataFrame()

#commence profiling

#'''
profile1 = pdp.ProfileReport(dbframe, title="Pandas Profiling Report: DataSet1")
profile2 = pdp.ProfileReport(csvframe, title="Pandas Profiling Report: DataSet2")
profile1.to_file("C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/ingestion_analysis/DataSet1Report.html")
profile2.to_file("C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/ingestion_analysis/DataSet2Report.html")
#'''

# This ended up as a dead end as part of an assignment using the project data
'''
# work for modifying csv files
csvheader = "ProjectName,DataSetName,Source,NumberOfRows,"
csvheader = csvheader + "NumberOfColumns,NumberOfStringCols,"
csvheader = csvheader + "NumberOfCategoricalCols,NumberOfIntegerCols,"
csvheader = csvheader + "NumberOfFloatCols,MissingData,Anomalies,"
csvheader = csvheader + "Active,JoinStrategy"

name = "CSCI 422 Project Gregory Beaucalir"
set1name = "aoepulseDB"
set2name = "aoe-elo"
url1 = "<https://www.aoepulse.com>"
url2 = "<https://aoe-elo.com>"

toWrite1 = name + "," + set1name + "," + url1 + ","
toWrite2 = name + "," + set2name + "," + url2 + ","

set1rows = set1frame.shape[0]
set1cols = set1frame.shape[1]

set2rows = set2frame.shape[0]
set2cols = set2frame.shape[1]

colcount1 = set1frame.dtypes.value_counts()
print(colcount1)

#'''
print("script complete")