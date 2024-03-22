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

# usage
track = "3qehmGY1DnlBI5W9iFlUhR"
recs = recommend_similar_songs(track)
print("Recommended songs for", track, "are: ", recs)


# Close the Neo4j driver
driver.close()
