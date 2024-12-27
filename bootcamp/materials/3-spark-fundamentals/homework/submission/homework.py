from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, mean, count_distinct, sum

spark = SparkSession.builder.appName("hw").getOrCreate()

# Disabled automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load data from csv files
matches_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("../data/matches.csv")
)
maps_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("../data/maps.csv")
)
medals_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("../data/medals.csv")
)
match_details_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("../data/match_details.csv")
)
medals_matches_players_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("../data/medals_matches_players.csv")
)

# Broadcast join
# btw matches and maps on mapid
matches_maps_df = matches_df.join(
    broadcast(
        maps_df.withColumnRenamed("name", "map_name").withColumnRenamed(
            "description", "map_description"
        )
    ),
    "mapid",
    how="left",
)
matches_maps_df.explain()

# btw medals_matches_players and medals on medal_id
medals_matches_players_medals_df = medals_matches_players_df.join(
    broadcast(
        medals_df.withColumnRenamed("name", "medal_name").withColumnRenamed(
            "description", "medal_description"
        )
    ),
    "medal_id",
    how="left",
)
matches_maps_df.explain()

# Bucket join
# Before bucketing, the plan uses SortMergeJoin with FileScan
match_details_df.join(matches_maps_df, "match_id", how="left").join(
    medals_matches_players_df, ["match_id", "player_gamertag"], how="left"
).explain()

# Bucket tables
matches_maps_df.write.mode("overwrite").format("parquet").bucketBy(
    16, "match_id"
).saveAsTable("bootcamp.matches_maps_bucketed")
match_details_df.write.mode("overwrite").format("parquet").bucketBy(
    16, "match_id"
).saveAsTable("bootcamp.match_details_bucketed")
medals_matches_players_medals_df.write.mode("overwrite").format("parquet").bucketBy(
    16, "match_id"
).saveAsTable("bootcamp.medals_matches_players_medals_bucketed")

# Read the data again but from bucketed tables
matches_maps_bucketed_df = spark.table("bootcamp.matches_maps_bucketed")
match_details_bucketed_df = spark.table("bootcamp.match_details_bucketed")
medals_matches_players_medals_bucketed_df = spark.table(
    "bootcamp.medals_matches_players_medals_bucketed"
)

# Join three tables on match_id, the plan now has SortMergeJoin, but with BatchScan
matches_obt_df = match_details_bucketed_df.join(
    matches_maps_bucketed_df, "match_id", how="left"
).join(
    medals_matches_players_medals_bucketed_df,
    ["match_id", "player_gamertag"],
    how="left",
)
matches_obt_df.explain()
matches_obt_df.cache()

# Which player averages the most kills per game?
result_1_df = (
    matches_obt_df.groupby("player_gamertag")
    .agg(mean("player_total_kills").alias("avg_kills_per_game"))
    .sort("avg_kills_per_game", ascending=False)
)
result_1_df.show(1)

# Which playlist gets played the most?
result_2_df = (
    matches_obt_df.groupby("playlist_id")
    .agg(count_distinct("match_id").alias("num_unique_matches"))
    .sort("num_unique_matches", ascending=False)
)
result_2_df.show(1)

# Which map gets played the most?
result_3_df = (
    matches_obt_df.groupby("map_name")
    .agg(count_distinct("match_id").alias("num_unique_matches"))
    .sort("num_unique_matches", ascending=False)
)
result_3_df.show(1)

# Which map do players get the most Killing Spree medals on?
result_4_df = (
    matches_obt_df.filter(matches_obt_df.medal_name == "Killing Spree")
    .groupby("map_name")
    .agg(sum("count").alias("medal_count"))
    .sort("medal_count", ascending=False)
)
result_4_df.show(1)

# With the aggregated data set, try different .sortWithinPartitions to see which has the smallest data size
start_df = matches_obt_df.repartition(4, col("completion_date"))
mapid_sort_df = start_df.sortWithinPartitions(col("mapid"))
mapid_playlistid_sort_df = start_df.sortWithinPartitions(
    col("mapid"), col("playlist_id")
)
mapid_playlistid_player_gamertag_sort_df = start_df.sortWithinPartitions(
    col("mapid"), col("playlist_id"), col("player_gamertag")
)

# Write dataframes to tables
start_df.write.mode("overwrite").saveAsTable("bootcamp.matches_unsorted")
mapid_sort_df.write.mode("overwrite").saveAsTable("bootcamp.matches_sorted_mapid")
mapid_playlistid_sort_df.write.mode("overwrite").saveAsTable(
    "bootcamp.matches_sorted_mapid_playlistid"
)
mapid_playlistid_player_gamertag_sort_df.write.mode("overwrite").saveAsTable(
    "bootcamp.matches_sorted_mapid_playlistid_player_gamertag"
)

# sql to get the size of the tables
# %%sql
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted'
# FROM demo.bootcamp.matches_unsorted.files
# UNION ALL
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_mapid'
# FROM demo.bootcamp.matches_sorted_mapid.files
# UNION ALL
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_mapid_playlistid'
# FROM demo.bootcamp.matches_sorted_mapid_playlistid.files
# UNION ALL
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_mapid_playlistid_player_gamertag'
# FROM demo.bootcamp.matches_sorted_mapid_playlistid_player_gamertag.files

# From the result, the dataframe sorted by mapid_playlistid_player_gamertag has the smallest data size of 19815602 with number of partition is 4.
