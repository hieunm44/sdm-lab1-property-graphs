from neo4j import GraphDatabase
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


def drop_graph1(session):
    query = '''
        CALL gds.graph.drop('graph1')
    '''
    session.run(query)


def drop_graph2(session):
    query = '''
        CALL gds.graph.drop('graph2')
    '''
    session.run(query)


# For PageRank
def create_graph1(session):
    query = '''
        CALL gds.graph.project('graph1', 'Paper', 'CITES')
    '''
    session.run(query)


# For triangle counting
def create_graph2(session):
    query = '''  
        CALL gds.graph.project('graph2', 'Paper', {CITES: {orientation: 'UNDIRECTED'}})
    '''
    session.run(query)

    
def pagerank(session):
    query = '''
        CALL gds.pageRank.stream('graph1')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS paper, score
        ORDER BY score DESC
    '''
    result = session.run(query)
    data = [(record['paper'], record['score']) for record in result]
    df = pd.DataFrame(data=data, columns=['paper', 'score'])

    return df

    
def triangle_count(session):
    query = '''
        CALL gds.triangleCount.stream('graph2')
        YIELD nodeId, triangleCount
        RETURN gds.util.asNode(nodeId).title AS title, triangleCount
        ORDER BY triangleCount DESC
    '''

    result = session.run(query)
    data = [(record['title'], record['triangleCount']) for record in result]
    df = pd.DataFrame(data=data, columns=['title', 'triangle count'])

    return df


if __name__ == '__main__':
    with driver.session() as session:
        driver.verify_connectivity()

        # drop_graph1(session)
        # create_graph1(session)
        # pr = pagerank(session)
        # print(f'PageRank:\n {pr}')
        drop_graph2(session)
        create_graph2(session)
        tc = triangle_count(session)
        print(f'Triangle Count:\n {tc}')
