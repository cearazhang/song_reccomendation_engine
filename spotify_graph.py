import pandas as pd
from neo4j import GraphDatabase


# Connect to Neo4j
uri = "neo4j://localhost:7687"
user = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Function to recommend similar songs for a given song
def recommend_similar_songs(track_id):
    with driver.session() as session:
        # Cypher query to find similar songs for a given song
        query = (
            "MATCH (s1:Track {track_id: $track_id})-[:SIMILAR_TO]->(s2:Track) "
            "RETURN s2.track_name AS similar_song, s2.artists AS artist "
            "LIMIT 5"
        )
        result = session.run(query, track_id=track_id)
        recommendations = [(record["similar_song"], record["artist"]) for record in result]
        return recommendations
# Function to execute cypher queries
def execute_query(query):
    with driver.session() as session:
        session.run(query)

# Function to import nodes
def import_nodes(df):
    for _, row in df.iterrows():
        track_id = row['track_id']
        query = f"CREATE (:Song {{track_id: '{track_id}'}})"
    execute_query(query)

# Function to import edges
def import_edges(df):
    for _, row in df.iterrows():
        track_id1 = row['track_id1']
        track_id2 = row['track_id2']
        sim_score= row['sim_score']
        query = f'MATCH (a:Song {{track_id: {track_id1}}}), (b:Song {{track_id: {track_id2}}}) CREATE (a)-[:SIMILAR_TO {{score: toFloat({sim_score})}}]->(b)'
    execute_query(query)

# import song and similairty data
sample_song_df = pd.read_csv('sample_songs.csv')
sample_song_sim_df =  pd.read_csv('sample_song_similarity.csv')

# import nodes and edges
import_nodes(sample_song_df)
import_edges(sample_song_sim_df)

# usage
track = "3qehmGY1DnlBI5W9iFlUhR"
recs = recommend_similar_songs(track)
print("Recommended songs for", track, "are: ", recs)


# Close the Neo4j driver
driver.close()
