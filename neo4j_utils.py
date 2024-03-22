def execute_query(driver, query):
    """
    Executes a cypher query in neo4j.

    Args:
        driver: driver for neo4j
        query (str): string cypher query.
    """
    with driver.session() as session:
        session.run(query)

def import_nodes(driver, df):
    """
    Imports all song nodes from the given dataframe into neo4j.

    Args:
        driver: driver for neo4j
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
        execute_query(driver, query)

def import_edges(driver, df):
    """
    Imports all song edges from the given dataframe into neo4j.

    Args:
        driver: driver for neo4j
        df (DataFrame): DataFrame of songs and their similarity scores.
    """
    for i, row in df.iterrows():
        track_id1 = row['track_id1']
        track_id2 = row['track_id2']
        sim_score= row['sim_score']
        query = f"MATCH (a:Song {{track_id: '{str(track_id1)}'}}), (b:Song {{track_id: '{str(track_id2)}'}}) CREATE (a)-[:Similar {{score: toFloat({sim_score})}}]->(b)"
        execute_query(driver, query)

def recommend_similar_songs(driver, track_id):
    """
    Reccommends top 5 songs similar to given track.

    Args:
        driver: driver for neo4j
        track_id (str): id of the track to find similar songs to.
    """
    query =   f"MATCH (source {{track_id:'{track_id}'}})-[r:Similar]->(target) WITH DISTINCT target, r.score AS score RETURN target.track_id AS track_id, target.track_name AS track_name, target.artists AS artists ORDER BY score DESC LIMIT 5"
    with driver.session() as session:
        result = session.run(query)
        result = list(result)
    recommendations = [(record["track_name"], record["artists"]) for record in result]
    return recommendations
