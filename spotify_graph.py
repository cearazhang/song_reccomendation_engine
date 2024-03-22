import pandas as pd
from neo4j import GraphDatabase


# Connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri)

# Function to execute cypher queries
def execute_query(query):
    with driver.session() as session:
        session.run(query)

# Function to import nodes
def import_nodes(df):
    df = df.copy()
    # remove duplicate songs
    df = df[['track_id', 'track_name', 'artists']].drop_duplicates()
    for _, row in df.iterrows():
        track_id = row['track_id']
        track_name = row['track_name']
        artists = row['artists']
        query = f"CREATE (:Song {{track_id: '{track_id}', track_name: '{track_name}', artists: '{artists}'}})"
    execute_query(query)

# Function to import edges
def import_edges(df):
    for _, row in df.iterrows():
        track_id1 = row['track_id1']
        track_id2 = row['track_id2']
        sim_score= row['sim_score']
        query = f"MATCH (a:Song {{track_id: '{str(track_id1)}'}}), (b:Song {{track_id: '{str(track_id2)}'}}) CREATE (a)-[:Similar {{score: toFloat({sim_score})}}]->(b)"
    execute_query(query)

# Function to recommend similar songs for a given song
def recommend_similar_songs(track_id):
    query = f"MATCH (source {{track_id:'{str(track_id)}'}})-[r:SIMILAR_TO]->(target) WITH DISTINCT target, r.score AS score RETURN target.track_id AS track_id, target.track_name AS track_name, target.artists AS artists ORDER BY score DESC LIMIT 5"
    with driver.session() as session:
        result = session.run(query)
        result = list(result)
    recommendations = [(record["track_name"], record["artists"]) for record in result]
    return recommendations

# import song and similairty data
sample_song_df = pd.read_csv('sample_songs.csv')
sample_song_sim_df =  pd.read_csv('sample_song_similarity.csv')

# import nodes and edges
import_nodes(sample_song_df)
import_edges(sample_song_sim_df)

# usage
track = "1J17nBsbh9HWVvtJFsAPYb"
recs = recommend_similar_songs(track)
print("Recommended songs for", track, "are: ", recs)

# delete databse for testing
execute_query("MATCH (n) DETACH DELETE n")

# Close the Neo4j driver
driver.close()
