from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, desc, sum as F_sum, countDistinct

# Enable Hive support for bucketed joins
spark = SparkSession.builder \
    .appName("SparkHomework") \
    .enableHiveSupport() \
    .getOrCreate()

# Set configurations
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.bucketing.enabled", "true")

# Ensure database exists
spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

# Load datasets with proper schema inference
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv") \
    .select(col("mapid"), col("name").alias("map_name"), col("description").alias("map_description"))
medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")

# Query 2: Explicit broadcast joins for medals and maps
# Example broadcast join between medals and maps (even though unrelated)
medals_broadcast = broadcast(medals)
maps_broadcast = broadcast(maps)

# Demo broadcast join (cross join for demonstration)
demo_broadcast_join = medals_broadcast.crossJoin(maps_broadcast).limit(5)
print("Demo broadcast join of medals and maps:")
demo_broadcast_join.show()

# Query 3: Bucket join across three tables on match_id with 16 buckets
def write_bucketed(df, table_name):
    (df.write
       .format("orc")
       .bucketBy(16, "match_id")
       .sortBy("match_id")
       .mode("overwrite")
       .saveAsTable(table_name))

# Prepare and write bucketed tables
matches_prepared = matches.select("match_id", "mapid", "playlist_id", "game_mode", "completion_date")
match_details_prepared = match_details.select("match_id", "player_gamertag", 
                                             col("player_total_kills").cast("int").alias("player_total_kills"))
mmp_prepared = medals_matches_players.select("match_id", "medal_id", 
                                            col("COUNT").cast("int").alias("medal_count"))

write_bucketed(matches_prepared, "bootcamp.matches_b16")
write_bucketed(match_details_prepared, "bootcamp.match_details_b16")
write_bucketed(mmp_prepared, "bootcamp.mmp_b16")

# Read back bucketed tables and perform bucket join
md_b = spark.table("bootcamp.match_details_b16")
m_b = spark.table("bootcamp.matches_b16")
mmp_b = spark.table("bootcamp.mmp_b16")

# 3-way bucket join
bucketed_join = (m_b.join(md_b, "match_id")
                   .join(mmp_b, "match_id"))

print("Bucketed join result (sample):")
bucketed_join.limit(10).show()

# Query 4: Analytical queries with proper casting and broadcast joins

# Query 4a: Player with highest average kills per game
md_casted = match_details.select(
    "player_gamertag",
    "match_id", 
    col("player_total_kills").cast("int").alias("player_total_kills")
)

res = (md_casted.groupBy("player_gamertag")
         .agg(F_sum("player_total_kills").alias("sum_kills"),
              countDistinct("match_id").alias("games"))
         .withColumn("avg_kills_per_game", col("sum_kills") / col("games"))
         .orderBy(desc("avg_kills_per_game"))
         .limit(1))

print("Player with highest average kills per game:")
res.show()

# Query 4b: Playlist with most plays
playlist_plays = matches.groupBy("playlist_id") \
    .agg(countDistinct("match_id").alias("plays")) \
    .orderBy(desc("plays")) \
    .limit(1)

print("Playlist with most plays:")
playlist_plays.show()

# Query 4c: Most played map
map_plays = matches.groupBy("mapid") \
    .agg(countDistinct("match_id").alias("plays")) \
    .orderBy(desc("plays")) \
    .limit(1)

print("Most played map:")
map_plays.show()

# Query 4d: Map with most Killing Spree medals (using broadcast join for medals)
medals_b = broadcast(medals.select("medal_id", "name"))
mmp_casted = medals_matches_players.select(
    "match_id",
    "medal_id", 
    col("COUNT").cast("int").alias("medal_count")
)

ks = (mmp_casted.join(medals_b, "medal_id")
        .where(col("name") == "Killing Spree")
        .groupBy("match_id")
        .agg(F_sum("medal_count").alias("ks_per_match")))

ks_by_map = (ks.join(broadcast(matches.select("match_id", "mapid")), "match_id")
               .groupBy("mapid")
               .agg(F_sum("ks_per_match").alias("ks_total"))
               .orderBy(desc("ks_total"))
               .limit(1))

result = ks_by_map.join(broadcast(maps.select("mapid", "map_name")), "mapid") \
                  .select("map_name", "ks_total")

print("Map with most Killing Spree medals:")
result.show()

# Query 5: Data size optimization with partitioning and sortWithinPartitions
# Define base dataframe for partitioning experiments
base_df = matches.select("match_id", "mapid", "playlist_id", "game_mode", "completion_date")

# Variant A: Partitioned by mapid
base_df.repartition("mapid").write.mode("overwrite").format("parquet").partitionBy("mapid").saveAsTable("bootcamp.by_mapid_unsorted")

base_df.repartition("mapid").sortWithinPartitions("match_id").write.mode("overwrite").format("parquet").partitionBy("mapid").saveAsTable("bootcamp.by_mapid_sorted")

# Variant B: Partitioned by playlist_id
base_df.repartition("playlist_id").write.mode("overwrite").format("parquet").partitionBy("playlist_id").saveAsTable("bootcamp.by_playlist_unsorted")

base_df.repartition("playlist_id").sortWithinPartitions("match_id").write.mode("overwrite").format("parquet").partitionBy("playlist_id").saveAsTable("bootcamp.by_playlist_sorted")

# Variant C: Partitioned by game_mode
base_df.repartition("game_mode").write.mode("overwrite").format("parquet").partitionBy("game_mode").saveAsTable("bootcamp.by_gamemode_unsorted")

base_df.repartition("game_mode").sortWithinPartitions("match_id").write.mode("overwrite").format("parquet").partitionBy("game_mode").saveAsTable("bootcamp.by_gamemode_sorted")

# Size measurement for Parquet tables
print("Size comparison results:")

print("Map ID partitioned tables:")
spark.sql("DESCRIBE FORMATTED bootcamp.by_mapid_sorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)
spark.sql("DESCRIBE FORMATTED bootcamp.by_mapid_unsorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)

print("Playlist ID partitioned tables:")
spark.sql("DESCRIBE FORMATTED bootcamp.by_playlist_sorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)
spark.sql("DESCRIBE FORMATTED bootcamp.by_playlist_unsorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)

print("Game mode partitioned tables:")
spark.sql("DESCRIBE FORMATTED bootcamp.by_gamemode_sorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)
spark.sql("DESCRIBE FORMATTED bootcamp.by_gamemode_unsorted").filter(col("col_name").isin("Num Files", "Total Size")).show(truncate=False)

print("Assignment completed successfully!")