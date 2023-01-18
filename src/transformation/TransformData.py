# Databricks notebook source
# Notebook for transforming ingested data using pyspark in Azure Databricks
# Build Connection

spark.conf.set(
 "fs.azure.account.key.gregorybeauclairassign1.dfs.core.windows.net",
 dbutils.secrets.get(scope="FinalProjectScope", key="CSCI422storage-key"))

uri = "abfss://final-project@gregorybeauclairassign1.dfs.core.windows.net"


# COMMAND ----------

# Begin transformations and filtering
# pull tournaments

tournamentdf = spark.read.option("multiline","true").json(uri+"/tournament.json")
tournamentdf = tournamentdf.select('id', 'name', 'prizemoney', 'end').where(tournamentdf.name != 'delete')
tournamentdf = tournamentdf.withColumnRenamed('id', 'tournament_id').withColumnRenamed('end', 'date_completed')

display(tournamentdf)

# COMMAND ----------

# pull tournament results

tournamentresultdf = spark.read.option('miltiline', 'true').json(uri+"/tournamentresults.json")
tournamentresultdf = tournamentresultdf.select('tournament', 'player', 'type').where(tournamentresultdf.player != 'null')
tournamentresultdf = tournamentresultdf.withColumnRenamed('tournament', 'tournament_id').withColumnRenamed('player', 'player_id').withColumnRenamed('type', 'placement')

display(tournamentresultdf)

# COMMAND ----------

# pull tournament stages

tournamentstagesdf = spark.read.option('miltiline', 'true').json(uri+"/tournamentstages.json")
tournamentstagesdf = tournamentstagesdf.select('id', 'name').where(tournamentstagesdf.name != "null").withColumnRenamed('id', 'stage_id')

display(tournamentstagesdf)

# COMMAND ----------

# pull set results

tournamentsetresultdf = spark.read.option('miltiline', 'true').json(uri+"/setresults.json")
tournamentsetresultdf = tournamentsetresultdf.select('id', 'tournament_id', 'stage_id', 'date', 'player_1_id', 'score_1', 'player_2_id', 'score_2').where(tournamentsetresultdf.stage_id != "null").withColumnRenamed('id', 'set_id').withColumnRenamed('score_1', 'player_1_score').withColumnRenamed('score_2', 'player_2_score')

display(tournamentsetresultdf)

# COMMAND ----------

# pull players from tournament data

tournamentplayerdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/players.csv")
tournamentplayerdf = tournamentplayerdf.select('id', 'name', 'rank', 'retired', 'tournaments_list').withColumnRenamed('id', 'tournament_player_id')

display(tournamentplayerdf)

# COMMAND ----------

# pull custom interfaceing records

namePulseIDdictdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/filter.csv")
display(namePulseIDdictdf)

# COMMAND ----------

# pull pulse players

pulseplayerdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/aoepulseplayers.csv")


display(pulseplayerdf)


# COMMAND ----------

# pull pulse openings
pulseopeningdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/aoepulseopenings.csv")
pulseopeningdf = pulseopeningdf.select('id', 'name').withColumnRenamed('id', 'opening_type').withColumnRenamed('name', 'opening_name')

display(pulseopeningdf)

# COMMAND ----------

# pull pulse matches
pulsematchdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/aoepulsematches.csv")
pulsematchdf = pulsematchdf.select('id', 'map_id', 'time').withColumnRenamed('id', 'match_id').withColumnRenamed('time', 'time_played')

# assign map names to ids using pulse game parser data

maps= {
    9: "Arabia",
    10: "Archipelago",
    11: "Baltic",
    12: "Black Forest",
    13: "Coastal",
    14: "Continental",
    15: "Crater Lake",
    16: "Fortress",
    17: "Gold Rush",
    18: "Highland",
    19: "Islands",
    20: "Mediterranean",
    21: "Migration",
    22: "Rivers",
    23: "Team Islands",
    24: "Full Random",
    25: "Scandinavia",
    26: "Mongolia",
    27: "Yucatan",
    28: "Salt Marsh",
    29: "Arena",
    30: "King of the Hill",
    31: "Oasis",
    32: "Ghost Lake",
    33: "Nomad",
    34: "Iberia",
    37: "Texas",
    38: "Italy",
    39: "Central America",
    40: "France",
    41: "Norse Lands",
    42: "Sea of Japan (East Sea)",
    67: "Acropolis",
    68: "Budapest",
    69: "Cenotes",
    70: "City of Lakes",
    71: "Golden Pit",
    72: "Hideout",
    73: "Hill Fort",
    74: "Lombardia",
    75: "Steppe",
    76: "Valley",
    77: "MegaRandom",
    78: "Hamburger",
    79: "CtR Random",
    80: "CtR Monsoon",
    81: "CtR Pyramid Descent",
    82: "CtR Spiral",
    83: "Kilimanjaro",
    84: "Mountain Pass",
    85: "Nile Delta",
    86: "Serengeti",
    87: "Socotra",
    88: "Amazon",
    89: "China",
    90: "Horn of Africa",
    91: "India",
    92: "Madagascar",
    93: "West Africa",
    94: "Bohemia",
    95: "Earth",
    96: "Canyons",
    97: "Enemy Archipelago",
    98: "Enemy Islands",
    99: "Far Out",
    100: "Front Line",
    101: "Inner Circle",
    102: "Motherland",
    103: "Open Plains",
    104: "Ring of Water",
    105: "Snakepit",
    106: "The Eye",
    107: "Australia",
    108: "Indochina",
    109: "Indonesia",
    110: "Strait of Malacca",
    111: "Philippines",
    112: "Bog Islands",
    113: "Mangrove Jungle",
    114: "Pacific Islands",
    115: "Sandbank",
    116: "Water Nomad",
    117: "Jungle Islands",
    118: "Holy Line",
    119: "Border Stones",
    120: "Yin Yang",
    121: "Jungle Lanes",
    122: "Alpine Lakes",
    123: "Bogland",
    125: "Ravines",
    124: "Mountain Ridge",
    126: "Wolf Hill",
    139: "Golden Swamp",
    140: "Four Lakes",
    141: "Land Nomad",
    142: "Battle On Ice",
    143: "El Dorado",
    144: "Fall of Axum",
    145: "Fall of Rome",
    147: "Amazon Tunnel",
    148: "Coastal Forest",
    149: "African Clearing",
    150: "Atacama",
    151: "Seize the Mountain",
    152: "Crater",
    153: "Crossroads",
    154: "Michi",
    155: "Team Moats",
    156: "Volcanic Island",
    157: "Acclivity",
    158: "Eruption",
    159: "Frigid Lake",
    160: "Greenland",
    161: "Lowland",
    162: "Marketplace",
    163: "Meadow",
    164: "Mountain Range",
    165: "Northern Isles",
    166: "Ring Fortress",
    167: "Runestones",
    168: "Aftermath",
    169: "Enclosed",
    170: "Haboob",
    171: "Kawasan",
    172: "Land Madness",
    173: "Sacred Springs",
    174: "Wade",
    175: "Morass",
    176: "Shoals"
}

mapdf = spark.createDataFrame(maps.items()).withColumnRenamed('_1', 'map_type').withColumnRenamed('_2', 'map_name')

pulsematchdf = pulsematchdf.join(mapdf, how='fullouter')
pulsematchdf = pulsematchdf.select('match_id', 'map_name', 'time_played').where(pulsematchdf.map_id == pulsematchdf.map_type)

display(pulsematchdf)

# COMMAND ----------

# pull pulse match players
pulsematchplayerdf = spark.read.option('sep', '|').option('header', True).csv(uri+"/aoepulsematch_players.csv")
pulsematchplayerdf = pulsematchplayerdf.select('id', 'player_id', 'match_id', 'opening_id', 'civilization', 'victory').withColumnRenamed('id', 'match_player_id').withColumnRenamed('opening_id', 'opening_type')

# reassign names to numeric civ values based upon game parser values from aoepulse game parser

civ_names =  {
        "Aztecs": "10285",
        "Bengalis": "10311",
        "Berbers": "10297",
        "Bohemians": "10309",
        "Britons": "10271",
        "Bulgarians": "10302",
        "Burgundians": "10306",
        "Burmese": "10300",
        "Byzantines": "10277",
        "Celts": "10283",
        "Chinese": "10276",
        "Cumans": "10304",
        "Dravidians": "10310",
        "Ethiopians": "10295",
        "Franks": "10272",
        "Goths": "10273",
        "Gurjaras": "10312",
        "Hindustanis": "10290",
        "Huns": "10287",
        "Incas": "10291",
        "Italians": "10289",
        "Japanese": "10275",
        "Khmer": "10298",
        "Koreans": "10288",
        "Lithuanians": "10305",
        "Magyars": "10292",
        "Malay": "10299",
        "Malians": "10296",
        "Mayans": "10286",
        "Mongols": "10282",
        "Persians": "10278",
        "Poles": "10308",
        "Portuguese": "10294",
        "Saracens": "10279",
        "Sicilians": "10307",
        "Slavs": "10293",
        "Spanish": "10284",
        "Tatars": "10303",
        "Teutons": "10274",
        "Turks": "10280",
        "Vietnamese": "10301",
        "Vikings": "10281"
    }

civs = {}
for name, value in civ_names.items():
    civs[int(value) - 10270] = name

civdf = spark.createDataFrame(civs.items()).withColumnRenamed('_1', 'civ_id').withColumnRenamed('_2', 'civ_name')

pulsematchplayerdf = pulsematchplayerdf.join(civdf, how='fullouter')
pulsematchplayerdf = pulsematchplayerdf.select('match_player_id', 'player_id', 'match_id', 'opening_type', 'civ_name', 'victory').where(pulsematchplayerdf.civilization == pulsematchplayerdf.civ_id)

display(pulsematchplayerdf)

# COMMAND ----------

# display all schemas so far
'''
tournamentdf.printSchema()
print()
tournamentresultdf.printSchema()
print()
tournamentstagesdf.printSchema()
print()
tournamentsetresultdf.printSchema()
print()
tournamentplayerdf.printSchema()
print()
namePulseIDdictdf.printSchema()
print()
pulseplayerdf.printSchema()
print()
pulseopeningdf.printSchema()
print()
pulsematchdf.printSchema()
print()
pulsematchplayerdf.printSchema()
print()
'''

# modify schema to be proper datatypes
tournamentdf = tournamentdf.selectExpr("cast(tournament_id as int) tournament_id",
    "cast(name as string) tournament_name",
    "date_completed")
#tournamentdf.printSchema()
print()

tournamentresultdf= tournamentresultdf.selectExpr("cast(tournament_id as int) tournament_id", "cast(player_id as int) player_id", "cast(placement as int) placement")
#tournamentresultdf.printSchema()
print()

tournamentstagesdf = tournamentstagesdf.selectExpr("cast(stage_id as int) stage_id", "name as stage_name")
#tournamentstagesdf.printSchema()
print()

tournamentsetresultdf = tournamentsetresultdf.selectExpr("cast(set_id as int) set_id", "cast(tournament_id as int) tournament_id", "cast(stage_id as int) stage_id",
                                                        "date as date_completed", "cast(player_1_id as int) player_1_id",
                                                        "cast(player_1_score as int) player_1_score",
                                                        "cast(player_2_id as int) player_2_id", "cast(player_2_score as int) player_2_score")
#tournamentsetresultdf.printSchema()
print()

tournamentplayerdf = tournamentplayerdf.selectExpr("cast(tournament_player_id as int) tournament_player_id", "name as tournament_player_name", "cast(rank as int) rank", "cast(retired as boolean) retired",
                                                  "tournaments_list")
#tournamentplayerdf.printSchema()
print()

namePulseIDdictdf = namePulseIDdictdf.selectExpr("name as player_name", "cast(id as int) pulse_id")
#namePulseIDdictdf.printSchema()
print()

pulseplayerdf = pulseplayerdf.selectExpr("cast(id as int) pulse_player_id", "name as pulse_player_name")
#pulseplayerdf.printSchema()
print()

pulseopeningdf = pulseopeningdf.selectExpr("cast(opening_type as int) opening_type", "opening_name")
#pulseopeningdf.printSchema()
print()

pulsematchdf = pulsematchdf.selectExpr("cast(match_id as int) match_id", "map_name", "time_played")
#pulsematchdf.printSchema()
print()

pulsematchplayerdf = pulsematchplayerdf.selectExpr("cast(match_player_id as int) match_player_id", "cast(player_id as int) pulse_player_id",
                                                  "cast(match_id as int) match_id", "cast(opening_type as int) opening_type",
                                                  "civ_name", "victory")
#pulsematchplayerdf.printSchema()
print()

# COMMAND ----------

# pare down data sets to only be referencing players in magic crossover
tournamentplayerdf = tournamentplayerdf.join(namePulseIDdictdf, tournamentplayerdf.tournament_player_name == namePulseIDdictdf.player_name, "leftsemi")
#display(tournamentplayerdf)

tournamentresultdf = tournamentresultdf.join(tournamentplayerdf, tournamentresultdf.player_id == tournamentplayerdf.tournament_player_id, 'leftsemi')
#display(tournamentresultdf)

tournamentdf = tournamentdf.join(tournamentresultdf, tournamentdf.tournament_id == tournamentresultdf.tournament_id, 'leftsemi')
#display(tournamentdf)

tournamentsetresultdf = tournamentsetresultdf.join(tournamentdf, tournamentsetresultdf.tournament_id == tournamentdf.tournament_id, "leftsemi")
#display(tournamentsetresultdf)



# COMMAND ----------

# convert tournamentsetresultdf to long format
p1df = tournamentsetresultdf.selectExpr("set_id", 'tournament_id', 'stage_id', 'date_completed', 'player_1_id as player_id', 'player_1_score as player_score')
p2df = tournamentsetresultdf.selectExpr("set_id", 'tournament_id', 'stage_id', 'date_completed', 'player_2_id as player_id', 'player_2_score as player_score')

tournamentsetresultdf = p1df.union(p2df)
display(tournamentsetresultdf)

# COMMAND ----------

#Integrate Match fields into match_player records
pulsematchdf = pulsematchdf.withColumnRenamed('match_id', 'match_player_match_id')
pulsematchplayerdf = pulsematchplayerdf.join(pulsematchdf, pulsematchplayerdf.match_id == pulsematchdf.match_player_match_id, 'left')
pulsematchplayerdf = pulsematchplayerdf.selectExpr('match_player_id', 'pulse_player_id', 'match_id', 'opening_type', 'civ_name', 'victory', 'map_name', 'time_played')

#Integrate opening names into match_player
pulseopeningdf = pulseopeningdf.withColumnRenamed('opening_type', 'opening_id')
pulsematchplayerdf = pulsematchplayerdf.join(pulseopeningdf, pulsematchplayerdf.opening_type == pulseopeningdf.opening_id, 'left')
pulsematchplayerdf = pulsematchplayerdf.selectExpr('match_player_id', 'pulse_player_id', 'match_id', 'opening_name', 'civ_name', 'victory', 'map_name', 'time_played')

display(pulsematchplayerdf)

# COMMAND ----------

# integrate tournament fields into tournament result
tournamentdf = tournamentdf.withColumnRenamed('tournament_id', 'id')
tournamentresultdf = tournamentresultdf.join(tournamentdf, tournamentresultdf.tournament_id == tournamentdf.id, 'left')
tournamentresultdf = tournamentresultdf.select('tournament_id', 'player_id', 'placement', 'tournament_name', 'date_completed')

display(tournamentresultdf)

# COMMAND ----------

# join separate player tables to have one central player table
playerdf = namePulseIDdictdf.join(tournamentplayerdf, namePulseIDdictdf.player_name == tournamentplayerdf.tournament_player_name, 'fullouter')

playerdf = playerdf.select('player_name', 'pulse_id', 'tournament_player_id', 'rank')

display(playerdf)

# COMMAND ----------

# add tournament stage names to set results
tournamentstagesdf = tournamentstagesdf.withColumnRenamed('stage_id', 'stage_type')
tournamentsetresultdf = tournamentsetresultdf.join(tournamentstagesdf, tournamentstagesdf.stage_type == tournamentsetresultdf.stage_id, 'full')
tournamentsetresultdf = tournamentsetresultdf.select('set_id', 'tournament_id', 'date_completed', 'player_id', 'player_score', 'stage_name')

# add tournament name to set results
tournamenttemp = tournamentdf.select('id', 'tournament_name')
tournamentsetresultdf = tournamentsetresultdf.join(tournamenttemp, tournamentsetresultdf.tournament_id == tournamenttemp.id, 'left')
tournamentsetresultdf = tournamentsetresultdf.select('set_id', 'tournament_name', 'date_completed', 'player_id', 'player_score', 'stage_name')
display(tournamentsetresultdf)

# COMMAND ----------

# write out frames to csv format files
'''
tournamentdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournaments")
tournamentresultdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentResult")
tournamentstagesdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentStages")
tournamentsetresultdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentSetResults")
tournamentplayerdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentPlayer")
namePulseIDdictdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvNameID")
pulseplayerdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulsePlayer")
pulseopeningdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulseOpenings")
pulsematchdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulseMatches")
matchplayeropening.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulseMatchPlayerOpening")
matchplayerciv.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulseMatchPlayerCiv")
matchplayervictory.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvMatchPlayerVictory")
'''

pulsematchplayerdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPulseMatchPlayer")
tournamentresultdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentResult")
tournamentsetresultdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvTournamentSetResults")
playerdf.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"/output/csvPlayer")
