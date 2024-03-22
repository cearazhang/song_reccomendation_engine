from neo4j_utils import execute_query, import_nodes, import_edges, recommend_similar_songs
from neo4j import GraphDatabase
import pandas as pd

# connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri)

# execute_query(driver, "MATCH (n) DETACH DELETE n")

# import songs and similarity data
sample_song_df = pd.read_csv('sample_songs.csv')
sample_song_sim_df =  pd.read_csv('sample_song_similarity.csv')

"""# import nodes and edges
import_nodes(driver, sample_song_df)
import_edges(driver, sample_song_sim_df)"""

# get reccomendations for all Strokes Songs
strokes_ids = sample_song_df[sample_song_df['artists']=='The Strokes'].track_id
recs = []
for id in strokes_ids:
    recs+=recommend_similar_songs(driver, id)
recs = set(recs)
recs = [r for r in recs if r[1]!='The Strokes'][0:5]
print("Your reccomendations are: ", recs)

"""# delete database for testing
execute_query("MATCH (n) DETACH DELETE n")"""

# close the Neo4j driver
driver.close()