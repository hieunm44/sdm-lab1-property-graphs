from neo4j import GraphDatabase
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


def top_3_most_cited_papers(session):
    query = '''
        MATCH (paper1:Paper)-[cites:CITES]->(paper2:Paper)
        WITH COUNT(cites) AS cite_count, paper2 ORDER BY cite_count DESC
        WITH COLLECT(paper2.title) AS cited_paper, cite_count
        RETURN cited_paper AS top_3_most_cited_papers, cite_count
        LIMIT 3
    '''
    result = session.run(query)
    data = [(record['top_3_most_cited_papers'], record['cite_count']) for record in result]
    df = pd.DataFrame(data=data, columns=['paper', 'number of citations'])
    
    return df


def authors_published_4_editions(session):
    query = '''
        MATCH (author:Author)-[:WRITES]->(paper:Paper)-[published:PUBLISHED_IN]->(proc:Proceeding)
        WITH author, proc, COUNT(paper.key) AS edition_count
        WHERE proc.key =~ 'conf.*' AND edition_count >= 4
        RETURN author.name AS author_name, proc.key AS conference, edition_count
        ORDER BY edition_count DESC
    '''
    result = session.run(query)
    data = [(record['author_name'], record['conference'], record['edition_count']) for record in result]
    df = pd.DataFrame(data=data, columns=['author', 'conference', 'number of editions'])

    return df


def impact_factor(session):
    # calculate impact factor in the year 2000
    query = '''
        MATCH (paper:Paper)-[:PUBLISHED_IN]->(journal:Journal)
        WHERE toInteger(journal.year)=1999 OR toInteger(journal.year)=1998
        WITH journal.name AS journal_title, size(COLLECT(paper)) AS n_papers, COLLECT(paper) AS papers_in_journal
        MATCH(paper1:Paper)-[cites:CITES]->(paper2:Paper)
        WHERE paper1 IN papers_in_journal
        WITH journal_title, COUNT(cites) AS n_cites, n_papers
        RETURN journal_title,  toFloat(n_cites)/n_papers AS impact_factor
        ORDER BY impact_factor DESC
    '''
    
    result = session.run(query)
    data = [(record['journal_title'], record['impact_factor']) for record in result]
    df = pd.DataFrame(data=data, columns=['journal', 'impact factor'])

    return df


def h_index(session):
    query = '''
        MATCH (author:Author)-[writes:WRITES]->(paper1:Paper)-[cites:CITES]->(paper2:Paper)
        WITH author, paper2, COLLECT([id(paper1), paper1.title]) AS rows
        WITH author, paper2, RANGE(1, SIZE(rows)) AS enum_rows
            UNWIND enum_rows AS erow
        WITH author, erow AS pos, COUNT(paper2) as n_cited
        WHERE n_cited >= pos 
        RETURN author.name as author_name, pos as h_index
        ORDER BY h_index DESC
    '''
    result = session.run(query)
    data = [(record['author_name'], record['h_index']) for record in result]
    df = pd.DataFrame(data=data, columns=['author', 'h-index'])

    return df


if __name__ == '__main__':
    with driver.session() as session:
        driver.verify_connectivity()
        
        # print(f'Top three most cited paper:\n {top_3_most_cited_papers(session)}')
        # print(f'Authors who have published in at least 4 editions:\n {authors_published_4_editions(session)}')
        print(f'Impact factor of journals:\n {impact_factor(session)}')
        # print(f'h-index of authors:\n {h_index(session)}')