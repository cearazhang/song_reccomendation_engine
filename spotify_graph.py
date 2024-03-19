import pandas as pd
from neo4j import GraphDatabase



# Connect to Neo4j
uri = "neo4j://localhost:7687"
username = ""
password = ""
driver = GraphDatabase.driver(uri, auth=(username, password))

# Create nodes and relationships in Neo4j
with driver.session() as session:
    for track_id in song_df.iterrows():
        session.write_transaction(create_song_nodes, row)

# Close the Neo4j driver
driver.close()
