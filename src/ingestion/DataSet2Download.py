# DataSet2Download.py - script to extract data from its source.
# This script ended up not really being used in the final project, but serves as an exaple of how to interface with the aoe-elo api

import json
import requests
import pandas as pd

print("DataSet2 ingestion")

# prepare to make request to aoe-elo api
baseRequest = "https://aoe-elo.com/api?request="

# get players
responsePlayers = requests.get(baseRequest + "players")
playerdataAsJson = json.loads(responsePlayers.content)

# get tournaments
responseTournaments = requests.get(baseRequest + "tournaments")
tournamentDataAsJson = json.loads(responseTournaments.content)

# place into data frames to facilitate transformation into csv files
players = pd.DataFrame(playerdataAsJson)
tournaments = pd.DataFrame(tournamentDataAsJson)

listPIDS = []
listTIDS = []

count=0

# request each player individually to get full info on player rather than summary
for i in players['id']:
    #count +=1
    #if(count > 10): break
    responseIndividualPlayer = requests.get(baseRequest + "player&id=" + str(i))
    listPIDS.append(json.loads(responseIndividualPlayer.content))

count=0  

# same for tournamnets  
for i in tournaments['id']:
    #count +=1
    #if(count > 10): break
    responseIndTourn = requests.get(baseRequest + "tournament&id=" + str(i))
    listTIDS.append(json.loads(responseIndTourn.content))
    
players = pd.DataFrame(listPIDS)
tournaments = pd.DataFrame(listTIDS)

basepath = "dataset2"

# make csv files for players and tournaments
players.to_csv(basepath + "/players.csv", sep="|")
tournaments.to_csv(basepath + "/tournaments.csv", sep="|")

print("download done")