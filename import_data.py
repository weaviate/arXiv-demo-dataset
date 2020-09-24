#!/usr/bin/env python
# coding: utf-8

# # ArXiv dataset with Weaviate

import weaviate
import json
import tqdm
import re
import time
import uuid

year_pattern = r'([1-2][0-9]{3})'

def get_client():
    client = weaviate.Client("http://localhost:8080")
    meta_info = client.get_meta()
    print(meta_info)
    return client

client = get_client()

def get_metadata():
    with open('./data/arxiv-metadata-oai.json', 'r') as f:
        for line in f:
            yield line

def get_metadata_list(max_size):
    data  = []
    with open('./data/arxiv-metadata-oai.json', 'r') as f:
        for line in f:
            if len(data) >= max_size:
                return data[:max_size]
            data.append(json.loads(line))
    if len(data) <= max_size:
        return data
    return data[:max_size]

def test_metadata():
    metadata = get_metadata()
    for paper in metadata:
        for k, v in json.loads(paper).items():
            print(f'{k}: {v}')
        break

def generate_uuid(identifier):
    """ Generate an uuid
    :param namespace: allows to make identifiers unique if they come from different source systems.
                        E.g. google maps, osm, ...
    :param identifier: that is used to generate the uuid
    :return: properly formed uuid in form of string
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, identifier))

def extract_year(paper_id):
    year = 2000 + int(paper_id[:2])
        
    return year

# get ids of categories
def get_ids_of_categories():
    categories_with_uuids = client.query.get.things("Category", ["id", "uuid"]).with_limit(2000).do()
    try: 
        categories_with_uuids = categories_with_uuids['data']['Get']['Things']['Category']
        categories_with_uuids_dict = {}
        for category in categories_with_uuids:
            categories_with_uuids_dict[category['id']] = category['uuid']
        return categories_with_uuids_dict
    except KeyError:
        print("Got an key error: ", result)
    except weaviate.UnexpectedStatusCodeException as usce:
        print("Got an error: ", usce.json, " with status code: ", usce.status_code)

def format_author_name(author):
    regex = re.compile(r'[\n\r\t\'\\\"\`]')
    result = regex.sub('', author)
    result = re.sub(r'\(.*\)', '', result)
    result = re.sub(r'[\(\[\{].*?[\)\]\}]', "", result)
    return result

def add_and_return_authors(data, batch_size=512, max_papers=float('inf'), max_import_items=float('inf'), authors_dict={}):
    batch = weaviate.ThingsBatchRequest()
    
    no_items_in_batch = 0
    no_imported = 0
    no_of_papers = 0
    
    for paper in data:
        if no_imported >= max_import_items:
            return authors_dict

        # paper = json.loads(paper)
        if paper["authors"] is not None:

            # remove everything between parentheses (twice for recursion)
            name = format_author_name(paper["authors"])
            authors = name.replace(' and ', ', ').split(', ')

            for author in authors:
                if author not in authors_dict:
                    # add author to batch
                    author_uuid = generate_uuid(author)
                    batch.add_thing({"name": author}, "Author", author_uuid)
                    no_items_in_batch += 1
                    authors_dict[author] = author_uuid

                if no_items_in_batch >= batch_size or (no_imported+no_items_in_batch) >= max_import_items:
                    
                    try:
                        result = client.batch.create_things(batch)
                        no_imported += no_items_in_batch
                        print('{} new authors imported in last batch, {} total authors imported.'.format(no_items_in_batch, no_imported))
                        batch = weaviate.ThingsBatchRequest()
                        no_items_in_batch = 0
                    except weaviate.UnexpectedStatusCodeException as usce:
                        print('Handle weaviate error: ', usce.status_code, usce.json)
                    except weaviate.ConnectionError as ce:
                        print('Handle networking error: ', ce)
                    except Exception as e:
                        print("Error: ", e)

        no_of_papers += 1
        if no_of_papers >= max_papers:
            try:
                result = client.batch.create_things(batch)
                no_imported += no_items_in_batch
                print('{} new authors imported in last batch, {} total authors imported.'.format(no_items_in_batch, no_imported))
            except weaviate.UnexpectedStatusCodeException as usce:
                print('Handle weaviate error: ', usce.status_code, usce.json)
            except weaviate.ConnectionError as ce:
                print('Handle networking error: ', ce)
            except Exception as e:
                print("Error: ", e)

            return authors_dict

    return authors_dict

def format_journal_name(a_string):
    splitted = re.split('([0-9]+)', a_string)
    journal_name = re.sub('[\"\'\n]', '', splitted[0])
    return journal_name

def add_and_return_journals(data, batch_size=512, max_papers=float('inf'), max_import_items=float('inf'), journals_dict={}):
    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0
    no_of_papers = 0
    
    for paper in data:
        if no_imported >= max_import_items:
            return

        # paper = json.loads(paper)

        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            journal_uuid = generate_uuid(journal_name)

            if journal_name not in journals_dict:
                batch.add_thing({"name": journal_name}, "Journal", journal_uuid)
                no_items_in_batch += 1
                journals_dict[journal_name] = journal_uuid

            if no_items_in_batch >= batch_size or (no_imported+no_items_in_batch) >= max_import_items:
                try:
                    result = client.batch.create_things(batch)
                    no_imported += no_items_in_batch
                    print('{} new journals imported in last batch, {} total journal imported.'.format(no_items_in_batch, no_imported))

                    batch = weaviate.ThingsBatchRequest()
                    no_items_in_batch = 0
                except weaviate.UnexpectedStatusCodeException as usce:
                    print('handle weaviate error: ', usce.status_code, usce.json)
                except weaviate.ConnectionError as ce:
                    print('handle networking error: ', ce)
                except Exception as e:
                    print("Error: ", e)

            no_of_papers += 1
            if no_of_papers >= max_papers:
                try:
                    result = client.batch.create_things(batch)
                    no_imported += no_items_in_batch
                    print('{} new journals imported in last batch, {} total journal imported.'.format(no_items_in_batch, no_imported))

                    batch = weaviate.ThingsBatchRequest()
                    no_items_in_batch = 0
                    return journals_dict
                except weaviate.UnexpectedStatusCodeException as usce:
                    print('handle weaviate error: ', usce.status_code, usce.json)
                except weaviate.ConnectionError as ce:
                    print('handle networking error: ', ce)
                except Exception as e:
                    print("Error: ", e)

    return journals_dict

def add_and_return_papers(data, categories_dict, journals_dict, authors_dict, max_papers=1000, batch_size=512, max_import_items=float('inf')):
    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    paper_authors_uuids_dict = {}
    
    for paper in data:
        if no_imported >= max_import_items:
            return paper_authors_uuids_dict

        # paper = json.loads(paper)
        paper_object = {}

        if paper["title"] is not None: paper_object["title"] = paper["title"].replace('\n', ' ')

        paper_uuid = generate_uuid(paper_object["title"])

        if paper["doi"] is not None: paper_object["doi"] = paper["doi"]
        if paper["journal-ref"] is not None: paper_object["journalReference"] = paper["journal-ref"]
        if paper["id"] is not None: paper_object["arxivId"] = paper["id"]
        if paper["submitter"] is not None: paper_object["submitter"] = paper["submitter"]
        if paper["abstract"] is not None: paper_object["abstract"] = paper["abstract"].replace('\n', ' ')
        if paper["comments"] is not None: paper_object["comments"] = paper["comments"]
        if paper["report-no"] is not None: paper_object["reportNumber"] = paper["report-no"]
        if paper["versions"] is not None: 
            paper_object["versionHistory"] = str(paper["versions"]).strip('[]')
            paper_object["lastestVersion"] = paper["versions"][-1]

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
            name = format_author_name(paper["authors"])
            authors = name.split(', ')
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
        
        if no_items_in_batch >= batch_size or (no_imported+no_items_in_batch) >= max_import_items:
            try:
                result = client.batch.create_things(batch)
                no_imported += no_items_in_batch
                print('{} new papers imported in last batch, {} total papers imported.'.format(no_items_in_batch, no_imported))
                batch = weaviate.ThingsBatchRequest()
                no_items_in_batch = 0
            except weaviate.UnexpectedStatusCodeException as usce:
                print('handle weaviate error: ', usce.status_code, usce.json)
            except weaviate.ConnectionError as ce:
                print('handle networking error: ', ce)
            except Exception as e:
                print("Error: ", e)

    return paper_authors_uuids_dict

def add_wrotepapers_cref(paper_authors_uuids_dict, batch_size=512):
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
            client.batch.add_references(batch)
            no_imported += no_items_in_batch
            print('{} new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(no_items_in_batch, no_imported))

            batch = weaviate.ReferenceBatchRequest()
            no_items_in_batch = 0
    
    client.batch.add_references(batch)
    no_imported += no_items_in_batch
    print('{} new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(no_items_in_batch, no_imported))

    return    

def add_data(categories_dict, max_papers=float('inf')):
    data = get_metadata_list(max_size=max_papers)
    categories_dict = get_ids_of_categories()
    journals_dict = add_and_return_journals(data, max_papers=max_papers)
    time.sleep(2)
    authors_dict = add_and_return_authors(data, max_papers=max_papers)
    time.sleep(2)
    paper_authors_uuids_dict = add_and_return_papers(data, categories_dict, journals_dict, authors_dict, max_import_items=max_papers)
    time.sleep(2)
    add_wrotepapers_cref(paper_authors_uuids_dict)

if __name__ == "__main__":
    #test_metadata()
    max_papers = 200
    data = get_metadata_list(max_size=max_papers)
    categories_dict = get_ids_of_categories()
    journals_dict = add_and_return_journals(data, max_papers=max_papers)
    time.sleep(2)
    authors_dict = add_and_return_authors(data, max_papers=max_papers)
    time.sleep(2)
    paper_authors_uuids_dict = add_and_return_papers(data, categories_dict, journals_dict, authors_dict, max_import_items=max_papers)
    time.sleep(2)
    add_wrotepapers_cref(paper_authors_uuids_dict)

# 1. batch add all the Authors (without refs)
# 2. batch add all the papers (without refs) and batch add all the hasAuthors refs
# 3. batch add all the wrotePapers refs