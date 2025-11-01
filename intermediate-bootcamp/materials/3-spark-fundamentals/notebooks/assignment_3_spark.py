# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
spark = SparkSession.builder.appName("SparkHomework").getOrCreate()

spark

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.bucketing.enabled", "1")

maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv").select(col("mapid"), col("name").alias("map_name"), col("description").alias("map_description"))
medals = spark.read.option("header","true").csv("/home/iceberg/data/medals.csv")
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")


# %%
print(maps.count())
maps.show()

# %%
print(medals_matches_players.count())
medals_matches_players.show()

# %%
print(matches.count())
matches.show()

# %%
print(medals.count())
medals.show()


# %%
from pyspark.sql.functions import bucket


matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
# matches.show()


# %%
%%sql
DROP TABLE IF EXISTS bootcamp.matches_bucketed

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING
  ) USING iceberg PARTITIONED BY (bucket(16, match_id))

# %%
matches.write.mode("append").bucketBy(16,"match_id").saveAsTable("bootcamp.matches_bucketed")

# %%
%%sql
SELECT
  COUNT(1)
FROM
  bootcamp.matches_bucketed.files

# %%

match_details = spark.read.option("header","true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
match_details.show()


# %%
%%sql
DROP TABLE IF EXISTS bootcamp.match_details_bucketed

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank INTEGER,
    spartan_rank INTEGER,
    previous_total_xp INTEGER,
    total_xp INTEGER,
    previous_csr_tier STRING,
    previous_csr_designation STRING,
    previous_csr INTEGER,
    previous_csr_percent_to_next_tier STRING,
    previous_csr_rank INTEGER,
    current_csr_tier STRING,
    current_csr_designation STRING,
    current_csr INTEGER,
    current_csr_percent_to_next_tier STRING,
    current_csr_rank INTEGER,
    player_rank_on_team STRING,
    player_finished BOOLEAN,
    player_average_life STRING,
    player_total_kills INTEGER,
    player_total_headshots INTEGER,
    player_total_weapon_damage FLOAT,
    player_total_shots_landed INTEGER,
    player_total_melee_kills INTEGER,
    player_total_melee_damage FLOAT,
    player_total_assassinations INTEGER,
    player_total_ground_pound_kills INTEGER,
    player_total_shoulder_bash_kills INTEGER,
    player_total_grenade_damage FLOAT,
    player_total_power_weapon_damage FLOAT,
    player_total_power_weapon_grabs INTEGER,
    player_total_deaths INTEGER,
    player_total_assists INTEGER,
    player_total_grenade_kills INTEGER,
    did_win INTEGER,
    team_id STRING
  ) USING iceberg PARTITIONED BY (bucket(16, match_id));

# %%
match_details.write.mode("append").bucketBy(16,"match_id").saveAsTable("bootcamp.match_details_bucketed")

# %%


# %%
%%sql
SELECT
  SUM(file_size_in_bytes) AS size,
  COUNT(1) AS num_files,
  'sorted'
FROM
  bootcamp.match_details_bucketed.files

# %%

medals_matches_players = spark.read.option("header","true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
# medals_matches_players.show()

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    COUNT INTEGER
  ) USING iceberg PARTITIONED BY (bucket(16, match_id))

# %%
medals_matches_players.write.mode("append").bucketBy(16,"match_id").saveAsTable("bootcamp.medals_matches_players_bucketed")

# %%
%%sql
SELECT
  SUM(file_size_in_bytes) AS size,
  COUNT(1) AS num_files,
  'sorted'
FROM
  bootcamp.medals_matches_players_bucketed.files

# %%
match_details_bucketed = spark.table("bootcamp.match_details_bucketed")
matches_bucketed = spark.table("bootcamp.matches_bucketed")
medals_matches_players_bucketed = spark.table("bootcamp.medals_matches_players_bucketed").select(col("match_id"), col("player_gamertag").alias("mmp_palyer_gamertag"), col("medal_id"))

# %%
from pyspark.sql.functions import broadcast

matches_bucketed_with_maps = matches_bucketed.join(broadcast(maps),"mapid")
matches_bucketed_with_maps.show()

# %%

df = match_details_bucketed.join(matches_bucketed_with_maps, "match_id").join(medals_matches_players_bucketed,"match_id").join(broadcast(medals),"medal_id")
df.show();
df.explain()

# %%
# Which player averages the most kills per game?
from pyspark.sql.functions import desc
from pyspark.sql import functions as F

aggregated = df.groupBy("match_details_bucketed.player_gamertag").agg(F.sum("player_total_kills").alias("total_kills"))
player_kills = aggregated.orderBy(desc("total_kills"))
player_kills.show()

player_with_most_kills = player_kills.first()["player_gamertag"] 
player_with_most_kills

# %%
# Which playlist gets played the most?
from pyspark.sql.functions import desc
from pyspark.sql import functions as F

aggregated = df.groupBy("playlist_id").agg(F.count("playlist_id").alias("playlist_count"))
playlist_count = aggregated.orderBy(desc("playlist_count"))
playlist_count.show();

playlist_with_most_plays = playlist_count.first()["playlist_id"] 
playlist_with_most_plays

# %%
# Which map gets played the most?
from pyspark.sql.functions import desc
from pyspark.sql import functions as F

aggregated = df.groupBy("mapid").agg(F.count("mapid").alias("map_count"))
map_count = aggregated.orderBy(desc("map_count"))
map_count.show();

map_with_most_plays = map_count.join(broadcast(maps),"mapid").first()["map_name"]
map_with_most_plays

# %%
# Which map do players get the most Killing Spree medals on?
from pyspark.sql.functions import desc
from pyspark.sql import functions as F

killing_spree_medal_id = medals.filter(F.col("name") == "Killing Spree").first()["medal_id"]
killing_spree_medal_id


# %%

aggregated = df.filter(F.col("medal_id") == killing_spree_medal_id).groupBy("mapid").agg(F.count("mapid").alias("map_count"))
map_count = aggregated.orderBy(desc("map_count"))
map_count.show();

map_with_most_killing_spree_medals = map_count.join(broadcast(maps),"mapid").first()["map_name"]
map_with_most_killing_spree_medals

# %%
start_df = df.repartition(16, col("mapid")).withColumn("mapid",col("mapid"))
    
first_sort_df = start_df.sortWithinPartitions(col("mapid"))

start_df.write.mode("overwrite").saveAsTable("bootcamp.map_games_unsorted")
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.map_games_sorted")


# %%
%%sql

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.map_games_sorted.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM demo.bootcamp.map_games_unsorted.files



# %%
start_df = df.repartition(16, col("playlist_id")).withColumn("playlist_id",col("playlist_id"))
    
first_sort_df = start_df.sortWithinPartitions(col("playlist_id"))

start_df.write.mode("overwrite").saveAsTable("bootcamp.playlist_games_unsorted")
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.playlist_games_sorted")


# %%
%%sql

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.playlist_games_sorted.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM demo.bootcamp.playlist_games_unsorted.files



# %%



