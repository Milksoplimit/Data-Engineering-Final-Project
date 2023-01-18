[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-c66648af7eb3fe8bc4f294546bfd86ef473780cde1dea487d3c4ff354943c9ae.svg)](https://classroom.github.com/online_ide?assignment_repo_id=9032128&assignment_repo_type=AssignmentRepo)
# CSCI 422 Project - <Gregory Beauclair>

## Ingestion
Data is pulled from aoe-elo.com and aoepulse.com.
Data from aoe-elo.com is obtained by downloading the database in json format here(https://aoe-elo.com/about) and then removing specific tables needed. Several of the tables can be retrieved using api calls but it was more efficient to do it by hand when all of the database is included in the download. (DataSet2Download.py is a good example of how the two tables available could be obtained using api calls)
Data from aoepulse.com is from the data dump they made onto the internet archive here(https://archive.org/details/aoepulse_db) and stored in the native db format. It is possible to download this data via api calls to internet archive (as shown in DataSet1Downlaod.py).
Several .config files are used for storing both credentials and file paths for the scripts to work properly.
A migic filter file to match up player names from aoe-elo and player ids from aoepulse is also required.
Only the subset of data pertaining to the players in the magic filter is included and uploaded. (DataSet1.py is the script for doing so)

## Transformation
Data is transformed using Azure Databricks Notebooks. The notebook is in TransformData.py
The csv and json files that are the data are transformed using pyspark dataframes and output to csv files. Some of the data in the AoePulse sourced tables needed to be translated to be more useful, and this (https://www.reddit.com/r/aoe2/comments/xj815r/aoepulse_database_dump_100_gb_5_million_games/) post on reddit was a good resource for finding those translations.
The entire transformation process is performed in the single notebook, and each time data needs to be transformed and output the notebook only needs to be run.
I created a job to run this as needed in Azure Databricks.
The Data is transformed into a star schema suitable for analysis in PowerBI.

## Serving
Data is written out to Azure blob storage in a star schema in csv files that correspond to the tables created in the transformation process.
The job/notebook used for transformation handles outputting the data to the blob storage.

## Analysis
Analysis was performed in Power Bi to generate some interesting visuals. There were a few surprises, such as Fast Castle Openings do the best overall for win differentials in regular play. All of the generated visuals are in the Analysis folder in a power bi report called FinalProject.pbix.

## Reflections
Overall this was a challenging project. I absolutely learned a lot about the data engineering lifecycle. There were a number of challenges with the AoePulse database, and having to treat it as an on-prem source for ingestion. There was also all of the translation work that had to be done manually since the ids and names of players in the data sets didn't match up very nicely. I had to rely on my knowledge of the community to try and match up names and ids using the leaderborads at https://www.ageofempires.com/
I definetly learned a lot about python as well. Before this project I had never touched python before and so this was a bit of a baptism by fire having to jump into this project.
Hopefully this is a quality project. In a perfect world I would have gotten more data from more sources, but the AoePulse database was a large and hard to work with set that really took up a lot of time. I am proud of what I was able to produce though.