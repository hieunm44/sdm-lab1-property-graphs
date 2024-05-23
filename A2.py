from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

def delete_all_nodes_edges(session):
    query = 'MATCH (n) DETACH DELETE n'
    session.run(query)


def create_paper_node(session):
    query = '''
            LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
            CREATE (
                paper:Paper {
                    title: csv.title,
                    key: csv.key,
                    journal: csv.journal,
                    conf: csv.crossref,
                    link: csv.ee,
                    cite: csv.cite,
                    keywords: csv.keywords
                }
            )
    '''
    session.run(query)


def create_author_node(session):
    query = '''
            LOAD CSV WITH HEADERS FROM 'file:///dblp_authors.csv' AS csv
            CREATE (
                author:Author {
                    name: csv.author,
                    key: csv.key
                }
            )
    '''
    session.run(query)


def create_proceeding_node(session):
    query = '''
            LOAD CSV WITH HEADERS FROM 'file:///dblp_proceedings.csv' AS csv
            CREATE (
                proc:Proceeding {
                    title: csv.title,
                    key: csv.key,
                    year: csv.year,
                    booktitle: csv.booktitle,
                    link: csv.ee
                }
            )
    '''
    session.run(query)


def create_book_node(session):
    query = '''
            MATCH (proc:Proceeding)
            WITH DISTINCT proc.booktitle as booktitle
                UNWIND booktitle AS x
                CREATE (book:Book {title:x})
    '''
    session.run(query)


def create_journal_node(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_journals.csv' AS csv
        CREATE (
            journal:Journal {
                name: csv.journal,
                volume: csv.volume,
                year: csv.year
            }
        )
    '''
    session.run(query)

    
def create_keyword_node(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_keywords.csv' AS csv
        CREATE (keyword:Keyword {word: csv.keyword})
    '''
    session.run(query)


def create_paper_in_journal_edge(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
        MATCH (
            journal:Journal {
                name: csv.journal,
                volume: csv.volume,
                year: csv.year
            }
        )
        MATCH(paper:Paper {key: csv.key})
        MERGE (paper)-[:PUBLISHED_IN]->(journal)
    '''
    session.run(query)


def create_paper_in_proceeding_edge(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
        MATCH (proc:Proceeding {key: csv.crossref})
        MATCH (paper:Paper {key: csv.key})
        MERGE (paper)-[:PUBLISHED_IN]->(proc)
    '''
    session.run(query)


def create_paper_cites_paper_edge(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
        UNWIND SPLIT(csv.cite, '|') as cited_key
        MATCH (paper1:Paper {key: csv.key})
        MATCH (paper2:Paper {key: cited_key})
        MERGE (paper1)-[:CITES]->(paper2)
    '''
    session.run(query)


def create_paper_has_keyword_edge(session):
    query = '''
        LOAD CSV WITH HEADERS FROM 'file:///dblp_papers.csv' AS csv
        UNWIND SPLIT(csv.keyword, '|') as kw
        MATCH (paper:Paper {key: csv.key})
        MATCH (keyword:Keyword {word: kw})
        MERGE (paper)-[:HAS_KW]-(keyword)
    '''
    session.run(query)


def create_proceeding_in_book_edge(session):
    query = '''
        MATCH (proc: Proceeding)
        WITH proc
        MATCH (book: Book {title: proc.booktitle})
        MERGE (proc)-[:PART_OF]-(book)
    '''
    session.run(query)


def create_author_writes_paper_edge(session):
    query = '''
        MATCH (author:Author)
        UNWIND SPLIT(author.key, '|') as key
        MATCH (paper:Paper {key: key})
        MERGE (author)-[:WRITES]->(paper);
    '''
    session.run(query)


if __name__ == '__main__':
    with driver.session() as session:
        driver.verify_connectivity()
        # delete_all_nodes_edges(session)

        create_paper_node(session)
        create_author_node(session)
        create_book_node(session)
        create_proceeding_node(session)
        create_keyword_node(session)
        create_journal_node(session)

        create_paper_in_journal_edge(session)
        create_paper_in_proceeding_edge(session)
        create_paper_cites_paper_edge(session)
        create_paper_has_keyword_edge(session)
        create_proceeding_in_book_edge(session)
        create_author_writes_paper_edge(session)