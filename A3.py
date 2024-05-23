from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

def create_paper_reviewedby_author_edge(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
        MATCH (paper:Paper {key: csv.key})
        UNWIND SPLIT(csv.reviewer, '|') as reviewer
        MATCH (author:Author {: reviewer})
        MERGE (paper)-[:REVIEWED_BY]->(author)
    '''
    session.run(query)


def set_affiliation(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_affiliations.csv' AS csv
        MATCH (author:Author {name: csv.author})
        SET author.affiliation = csv.affiliation
    '''
    session.run(query)


def set_suggested_decision(session):
    query = '''
        MATCH (paper:Paper)-[review:REVIEWED_BY]-(author:Author)
        WITH review,(rand()) AS accept_prob
        WITH review, accept_prob,
        CASE
            WHEN accept_prob>0.5 THEN True
            ELSE False
        END AS decision
        SET review.suggested_decision=decision
    '''
    session.run(query)


if __name__ == '__main__':
    with driver.session() as session:
        driver.verify_connectivity()
        
        create_paper_reviewedby_author_edge(session)
        set_affiliation(session)
        set_suggested_decision(session)