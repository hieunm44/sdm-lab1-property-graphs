from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


def community(session):
    '''
    For a book, the keyword percentage is defined as the ratio of the number of papers having pre-defined keywords over the total number of papers.
    A book is considered to belong to a community if the keyword percentage is at least 0.9 (i.e. at least half of the papers has community keywords).
    '''
    
    query = '''
        MATCH (keyword:Keyword)<-[:HAS_KW]-(paper_has_kw:Paper)-[:PUBLISHED_IN]->(proceeding)-[:PART_OF]->(book:Book)
        WHERE keyword.word in ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
        WITH book, COUNT(DISTINCT paper_has_kw) AS n_papers_has_kw 
        MATCH (book)<-[:PART_OF]-(proceeding)<-[:PUBLISHED_IN]-(paper_in_book:Paper)
        WITH book, n_papers_has_kw, COUNT(paper_in_book) as n_papers_in_book
        WITH book, n_papers_has_kw, n_papers_in_book, (tofloat(n_papers_has_kw)/n_papers_in_book) as kw_percent
        WHERE kw_percent >= 0.3
        WITH book.title as book_title, n_papers_has_kw, n_papers_in_book, kw_percent
        ORDER BY kw_percent DESC
        RETURN COLLECT (book_title)
    '''
    result = session.run(query)

    return result.single()[0]


def top_100_papers(session, community):
    query = '''
        MATCH (paper1:Paper)-[cites:CITES]->(paper2:Paper)
        WITH COUNT(cites) AS cite_count, paper2 ORDER BY cite_count DESC
        WITH COLLECT(paper2.title) AS cited_paper, cite_count
        MATCH (book:Book)<-[:inBook]-(:Proceeding)<-[:published]-(paper)
        WHERE book.title IN '''+str(community)+''' 
        WITH paper2.title as top_paper, book.title as book_title, cite_count ORDER BY cite_count DESC
        RETURN COLLECT(top_paper)
        LIMIT 100
    '''
    result = session.run(query)

    return result.single()[0]


def gurus(session, top_100_papers):
    query = '''
    MATCH (paper:Paper)<-[:WRITES]-(author:Author)
    WHERE paper.title IN '''+str(top_100_papers)+'''
    WITH author.name as author_name, COUNT(paper) AS paper_count
    WHERE paper_count >= 2
    RETURN COLLECT(author_name)
    '''
    result = session.run(query)
    return result.single()[0]


if __name__ == '__main__':
    with driver.session() as session:
        driver.verify_connectivity()

        community = community(session)
        print(f'Community: {community}')
        # top_100_papers = top_100_papers(session, community)
        # print(f'Top 100 papers: {top_100_papers}')
        # gurus = gurus(session, top_100_papers)
        # print(f'Gurus: {gurus}')