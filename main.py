from neo4j_utils import execute_query, import_nodes, import_edges, recommend_similar_songs
from neo4j import GraphDatabase
import pandas as pd

# connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri)

# import songs and similarity data
sample_song_df = pd.read_csv('sample_songs.csv')
sample_song_sim_df =  pd.read_csv('sample_song_similarity.csv')
# filter to reduce number of rows
sample_song_sim_df = sample_song_sim_df[(sample_song_sim_df['sim_score']>0.95) & (sample_song_sim_df['sim_score']<0.955)]

# import nodes and edges
import_nodes(driver, sample_song_df)
import_edges(driver, sample_song_sim_df)

# get reccomendations for all Strokes Songs
strokes_ids = sample_song_df[sample_song_df['artists']=='The Strokes'].track_id
recs = []
for id in strokes_ids:
    recs+=recommend_similar_songs(driver, id)
recs = set(recs)
print(recs)

# delete database for testing
execute_query("MATCH (n) DETACH DELETE n")

# close the Neo4j driver
driver.close()