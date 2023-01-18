# DataSet1Cleanup.py - script for cleaning dataset1 so that is can be uploaded to ADLS
# This is essentially the ingestion step for the 'on-prem' database

import sqlite3
import pandas as pd
import os

print("running script")

# get path to current workspace to pull the path to the db from the config file
here = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(here, 'IAKey.config')

# get the secrets
keyFile = open(filename)
key = keyFile.readlines()

#Get connection to db
dbpath = key[4].strip()
connection = sqlite3.connect(dbpath)
querey = connection.cursor()

# pare down data from database using magic filter file
csvfilterframe = pd.read_csv(key[5].strip(), sep="|")
idlist = list(csvfilterframe.id)

sep = ','
listasstring = ''
for x in idlist: listasstring += (str(x) + sep)
listasstring = listasstring[:-1]

basepath = key[6]

# These string blocks are meant to be run one at a time using commenting out of the first part of the string #'''
# It is a very basic hacky way to have a notebooks like experience while not actually using a notebook

'''
dbplayersframe = pd.read_sql("select * from players where id in (" + listasstring + ")", connection )
dbplayersframe.to_csv(basepath + "aoepulseplayers.csv", sep="|")
#'''


'''
dbmatchplayerframe = pd.read_sql("select * from match_players where player_id in (" + listasstring + ")", connection)
dbmatchplayerframe.to_csv(basepath + "aoepulsematch_players.csv", sep="|")

matcheslist = list(dbmatchplayerframe.match_id)
sep = ','
listasstring = ''
for x in matcheslist: listasstring += (str(x) + sep)
listasstring = listasstring[:-1]

dbmatchframe = pd.read_sql("select * from matches where id in (" + listasstring + ")", connection)
dbmatchframe.to_csv(basepath + "aoepulsematches.csv", sep="|")
#'''

"""
dbopeningsframe = pd.read_sql("Select * from openings", connection)
dbopeningsframe.to_csv(basepath + "aoepulseopenings.csv", sep="|")
#"""

print("finished script")