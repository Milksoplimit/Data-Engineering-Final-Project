# DataSet1Download.py - script to extract data from its source.

import os
import pandas as pd
import internetarchive as ia
from zipfile import ZipFile
#import sqlite3

print("DataSet1 ingestion")

# get path to current workspace to pull the secret key from the config file
here = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(here, 'IAKey.config')

# get the key
keyFile = open(filename)
key = keyFile.readlines()

#print(key)

#'''
#make a request to the internet archive for the resource
config = {'s3' : {'access' : key[0].strip() , 'secret' : key[1].strip()}}
item = ia.get_item('aoepulse_db', config)

# download the resource into the directory
data = item.download(destdir="dataset1")

#print(data)
#'''

#'''
# unzip the resource to prepare it for upload
file = ZipFile("C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset1/aoepulse_db/aoepulse_db.zip", 'r')
file.extractall("C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset1/aoepulse_db/")
#'''

# old test code
'''
path = "C:/Users/Gregory Beauclair/Documents/GitHub/project-Milksoplimit/dataset1/aoepulse_db/Sep_18_2022_aoepulse.db"
connection = sqlite3.connect(path)

querey = connection.cursor()
'''

print('finished downloading')