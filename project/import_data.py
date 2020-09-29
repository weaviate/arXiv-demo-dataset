import weaviate
import json
import tqdm
import re
from project.helper import *

def add_and_return_journals(client, data: list, batch_size: int=512, n_papers: float=float('inf'), skip_n_papers: bool=False) -> dict:
    # add groups to weaviate
    log('Start adding Journals')

    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    journals_dict = {}
    
    for paper in data[skip_n_papers:n_papers+skip_n_papers]:
        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            journal_uuid = generate_uuid('Journal', journal_name)

            if journal_name not in journals_dict:
                batch.add_thing({"name": journal_name}, "Journal", journal_uuid)
                no_items_in_batch += 1
                journals_dict[journal_name] = journal_uuid

            if no_items_in_batch >= batch_size:
                send_batch(client, 'Journal', batch, no_imported)
                no_imported += no_items_in_batch
                batch = weaviate.ThingsBatchRequest()
                no_items_in_batch = 0

    if no_items_in_batch > 0:
        send_batch(client, 'Journal', batch, no_imported)

    time.sleep(2)
    return journals_dict

def add_and_return_authors(client, data: list, batch_size: int=512, n_papers: float=float('inf'), skip_n_papers: bool=False) -> dict:
    log('Start adding Authors')

    batch = weaviate.ThingsBatchRequest()
    
    no_items_in_batch = 0
    no_imported = 0
    
    authors_dict = {}

    for paper in data[skip_n_papers:n_papers+skip_n_papers]:
        if paper["authors"] is not None:
            # remove everything between parentheses (twice for recursion)
            authors = format_author_name(paper["authors"])

            for author in authors:
                if author not in authors_dict:
                    # add author to batch
                    author_uuid = generate_uuid('Author', author)
                    batch.add_thing({"name": author}, "Author", author_uuid)
                    no_items_in_batch += 1
                    authors_dict[author] = author_uuid

                if no_items_in_batch >= batch_size:
                    send_batch(client, 'Author', batch, no_imported)
                    no_imported += no_items_in_batch
                    batch = weaviate.ThingsBatchRequest()
                    no_items_in_batch = 0

    if no_items_in_batch > 0:
        send_batch(client, 'Author', batch, no_imported)

    time.sleep(2)
    return authors_dict


def add_and_return_papers(client, data: list, categories_dict: dict, journals_dict: dict, authors_dict: dict, batch_size: int=512, n_papers: float=float('inf'), skip_n_papers: bool=False) -> dict:
    log('Start adding Papers')

    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    paper_authors_uuids_dict = {}
    

    for paper in data[skip_n_papers:n_papers+skip_n_papers]:        
        paper_object = {}

        uuid_base = ""
        if paper["title"] is not None: 
            paper_object["title"] = paper["title"].replace('\n', ' ')
            uuid_base += paper_object["title"]
        if paper["doi"] is not None: 
            paper_object["doi"] = paper["doi"]
            uuid_base += paper["doi"]
        if paper["journal-ref"] is not None: paper_object["journalReference"] = paper["journal-ref"]
        if paper["id"] is not None: 
            paper_object["arxivId"] = paper["id"]
            uuid_base +=paper["id"]
        if paper["submitter"] is not None: paper_object["submitter"] = paper["submitter"]
        if paper["abstract"] is not None: paper_object["abstract"] = paper["abstract"].replace('\n', ' ')
        if paper["comments"] is not None: paper_object["comments"] = paper["comments"]
        if paper["report-no"] is not None: paper_object["reportNumber"] = paper["report-no"]
        if paper["versions"] is not None: 
            paper_object["versionHistory"] = str(paper["versions"]).strip('[]')
            paper_object["lastestVersion"] = paper["versions"][-1]
        
        paper_uuid = generate_uuid('Paper', paper_object["title"])

        # try to extract year
        if paper["id"] is not None:
            year = extract_year(paper["id"])
            paper_object["year"] = year

        if paper["categories"][0] is not None:
            categories_object = []
            for category in paper["categories"][0].split(' '): # id of category
                # create beacon
                if category not in categories_dict:
                    break
                beacon_url = "weaviate://localhost/things/" + categories_dict[category]
                beacon = {"beacon": beacon_url}
                categories_object.append(beacon)

            if len(categories_object) > 0:
                paper_object["hasCategories"] = categories_object

        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            # create beacon
            if journal_name in journals_dict:
                beacon_url = "weaviate://localhost/things/" + journals_dict[journal_name]
                beacon = {"beacon": beacon_url}
                paper_object['inJournal'] = [beacon]

        if paper["authors"] is not None:
            authors = format_author_name(paper["authors"])
            authors_object = []
            authors_uuid_list = []

            for author in authors:
                if author not in authors_dict:
                    break

                beacon_url = "weaviate://localhost/things/" + authors_dict[author]
                beacon = {"beacon": beacon_url}
                authors_object.append(beacon)
                authors_uuid_list.append(authors_dict[author])

            if len(authors_object) > 0:
                paper_object['hasAuthors'] = authors_object
                paper_authors_uuids_dict[paper_uuid] = authors_uuid_list

        batch.add_thing(paper_object, "Paper", paper_uuid)
        no_items_in_batch += 1
        
        if no_items_in_batch >= batch_size:
            send_batch(client, 'Paper', batch, no_imported)
            no_imported += no_items_in_batch
            batch = weaviate.ThingsBatchRequest()
            no_items_in_batch = 0
    
    if no_items_in_batch > 0:
        send_batch(client, 'Paper', batch, no_imported)

    time.sleep(2)
    return paper_authors_uuids_dict

def add_wrotepapers_cref(client, paper_authors_uuids_dict: dict, batch_size: int=512) -> dict:
    log('Start adding crefs from Authors:wrotePapers to Papers')

    batch = weaviate.ReferenceBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    for paper, authors in paper_authors_uuids_dict.items():
        for author in authors:
            beacon_url = "weaviate://localhost/things/" + paper
            beacon = {"beacon": beacon_url}

            batch.add_reference(author, "Author", "wrotePapers", paper)
            no_items_in_batch += 1

        if no_items_in_batch >= batch_size:
            results = client.batch.add_references(batch)
            for result in results: 
                if result['result']['status'] != 'SUCCESS':
                    log(result['result'])
            no_imported += no_items_in_batch
            log('{} new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(no_items_in_batch, no_imported))

            batch = weaviate.ReferenceBatchRequest()
            no_items_in_batch = 0
    
    results = client.batch.add_references(batch)
    for result in results: 
        if result['result']['status'] != 'SUCCESS':
            log(result['result'])
    no_imported += no_items_in_batch
    log('{} new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(no_items_in_batch, no_imported))
    
    time.sleep(2)
    return 