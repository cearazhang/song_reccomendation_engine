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

def import_nodes(df):
    """
    Imports all song nodes from the given dataframe into neo4j.

    Args:
        df (DataFrame): DataFrame of songs
    """
    df = df.copy()
    # remove duplicate songs
    df = df[['track_id', 'track_name', 'artists']].drop_duplicates()
    for _, row in df.iterrows():
        track_id = row['track_id']
        track_name = row['track_name']
        artists = row['artists']
        query = f"CREATE (:Song {{track_id: '{track_id}', track_name: '{track_name}', artists: '{artists}'}})"
        execute_query(query)

def import_edges(df):
    """
    Imports all song edges from the given dataframe into neo4j.

    Args:
        df (DataFrame): DataFrame of songs and their similarity scores.
    """
    for _, row in df.iterrows():
        track_id1 = row['track_id1']
        track_id2 = row['track_id2']
        sim_score= row['sim_score']
        query = f"MATCH (a:Song {{track_id: '{str(track_id1)}'}}), (b:Song {{track_id: '{str(track_id2)}'}}) CREATE (a)-[:Similar {{score: toFloat({sim_score})}}]->(b)"
        execute_query(query)

def recommend_similar_songs(track_id):
    """
    Reccommends top 5 songs similar to given track.

    Args:
        track_id (str): id of the track to find similar songs to.
    """
    query =   f"MATCH (source {{track_id:'{track_id}'}})-[r:Similar]->(target) WITH DISTINCT target, r.score AS score RETURN target.track_id AS track_id, target.track_name AS track_name, target.artists AS artists ORDER BY score DESC LIMIT 5"
    with driver.session() as session:
        result = session.run(query)
        result = list(result)
    recommendations = [(record["track_name"], record["artists"]) for record in result]
    return recommendations

# import songs and similarity data
sample_song_df = pd.read_csv('sample_songs.csv')
sample_song_sim_df =  pd.read_csv('sample_song_similarity.csv')

# import nodes and edges
import_nodes(sample_song_df)
import_edges(sample_song_sim_df)

# usage
track = "0wnLMEOQlyW3Es1ag0HMVV"
recs = recommend_similar_songs(track)
print("Recommended songs for", track, "are: ", recs)

# delete databse for testing
execute_query("MATCH (n) DETACH DELETE n")

# Close the Neo4j driver
driver.close()
