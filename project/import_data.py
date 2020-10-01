import os
import weaviate
import json
import tqdm
import re
import time
from dateutil import parser
try:
    from project.helper import *
except ModuleNotFoundError:
    from helper import *


def add_and_return_journals(
        client: weaviate.client.Client,
        data: list,
        batch_size: int = 512,
        n_papers: float = 1000000000) -> dict:
    """ Adds journals of the papers to weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param data: the metadata of all papers to add
    :type data: list
    :param batch_size: number of items in a batch, defaults to 512
    :type batch_size: int, optional
    :param n_papers: number of papers to import in total, defaults to 1000000000
    :type n_papers: float, optional
    :return: journals with uuids
    :rtype: dict
    """
    # add groups to weaviate
    log('Start adding Journals')

    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    journals_dict = {}

    for paper in data:
        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            journal_uuid = generate_uuid('Journal', journal_name)

            if journal_name not in journals_dict:
                batch.add_thing({"name": journal_name},
                                "Journal", journal_uuid)
                no_items_in_batch += 1
                journals_dict[journal_name] = journal_uuid

            if no_items_in_batch >= batch_size:
                no_imported = send_batch(client, 'Journal', batch, no_imported)
                # no_imported += no_items_in_batch
                batch = weaviate.ThingsBatchRequest()
                no_items_in_batch = 0

    if no_items_in_batch > 0:
        no_imported = send_batch(client, 'Journal', batch, no_imported)

    log('Done adding Journals')
    time.sleep(2)
    return journals_dict


def add_and_return_authors(
        client: weaviate.client.Client,
        data: list,
        batch_size: int = 512,
        n_papers: float = 1000000000) -> dict:
    """[summary]

    :param client: python client connection
    :type client: weaviate.client.Client
    :param data: the metadata of all papers to add
    :type data: list
    :param batch_size: number of items in a batch, defaults to 512
    :type batch_size: int, optional
    :param n_papers: number of papers to import in total, defaults to 1000000000
    :type n_papers: float, optional
    :return: authods with uuids
    :rtype: dict
    """
    log('Start adding Authors')

    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    authors_dict = {}

    for paper in data:
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
                    no_imported = send_batch(client, 'Author', batch, no_imported)
                    # no_imported += no_items_in_batch
                    batch = weaviate.ThingsBatchRequest()
                    no_items_in_batch = 0

    if no_items_in_batch > 0:
        no_imported = send_batch(client, 'Author', batch, no_imported)

    log('Done adding Authors')
    time.sleep(2)
    return authors_dict


def add_and_return_papers(
        client: weaviate.client.Client,
        data: list,
        categories_dict: dict,
        journals_dict: dict,
        authors_dict: dict,
        batch_size: int = 512,
        n_papers: float = 1000000000) -> dict:
    """[summary]

    :param client: python client connection
    :type client: weaviate.client.Client
    :param data: the metadata of all papers to add
    :type data: list
    :param categories_dict: categories with uuids
    :type categories_dict: dict
    :param journals_dict: journals with uuids
    :type journals_dict: dict
    :param authors_dict: authors with uuids
    :type authors_dict: dict
    :param batch_size: number of items in a batch, defaults to 512
    :type batch_size: int, optional
    :param n_papers: number of papers to import in total, defaults to 1000000000
    :type n_papers: float, optional
    :return: uuids of all papers with uuids the authors
    :rtype: dict
    """
    log('Start adding Papers')

    batch = weaviate.ThingsBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    paper_authors_uuids_dict = {}

    for paper in data:
        paper_object = {}

        uuid_base = ""
        if paper["title"] is not None:
            paper_object["title"] = paper["title"].replace('\n', ' ')
            uuid_base += paper_object["title"]
        if paper["doi"] is not None:
            paper_object["doi"] = paper["doi"]
            uuid_base += paper["doi"]
        if paper["journal-ref"] is not None:
            paper_object["journalReference"] = paper["journal-ref"]
        if paper["id"] is not None:
            paper_object["arxivId"] = paper["id"]
            uuid_base += paper["id"]
        if paper["submitter"] is not None:
            paper_object["submitter"] = paper["submitter"]
        if paper["abstract"] is not None:
            paper_object["abstract"] = paper["abstract"].replace('\n', ' ')
        if paper["comments"] is not None:
            paper_object["comments"] = paper["comments"]
        if paper["report-no"] is not None:
            paper_object["reportNumber"] = paper["report-no"]
        if paper["versions"] is not None:
            if isinstance(paper["versions"][0], str):  # older arxiv datadump files use versions in a string in one list item
                paper_object["versionHistory"] = str(paper["versions"]).strip('[]')
                paper_object["latestVersion"] = paper["versions"][-1]
                uuid_base += paper_object["latestVersion"]
            elif isinstance(paper["versions"][0], dict):  # lastest arxiv datadump file uses different version datatypes
                latest_version_number = 0
                version_history = []
                for version in paper["versions"]:
                    version_number = int(version["version"].split('v')[1])
                    if version_number >= latest_version_number:
                        paper_object["latestVersion"] = version["version"]
                        try:
                            paper_object["latestVersionCreated"] = parser.parse(version["created"]).isoformat()
                        except Exception:
                            pass
                    version_history.append(version["version"])
                paper_object["versionHistory"] = ','.join(map(str, version_history))
                uuid_base += paper_object["latestVersion"]

        paper_uuid = generate_uuid('Paper', paper_object["title"])

        # try to extract year
        if paper["id"] is not None:
            year = extract_year(paper["id"])
            paper_object["year"] = year

        if paper["categories"][0] is not None:
            categories_object = []
            for category in paper["categories"][0].split(
                    ' '):  # id of category
                # create beacon
                if category not in categories_dict:
                    break
                beacon_url = "weaviate://localhost/things/" + \
                    categories_dict[category]
                beacon = {"beacon": beacon_url}
                categories_object.append(beacon)

            if len(categories_object) > 0:
                paper_object["hasCategories"] = categories_object

        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            # create beacon
            if journal_name in journals_dict:
                beacon_url = "weaviate://localhost/things/" + \
                    journals_dict[journal_name]
                beacon = {"beacon": beacon_url}
                paper_object['inJournal'] = [beacon]

        if paper["authors"] is not None:
            authors = format_author_name(paper["authors"])
            authors_object = []
            authors_uuid_list = []

            for author in authors:
                if author not in authors_dict:
                    break

                beacon_url = "weaviate://localhost/things/" + \
                    authors_dict[author]
                beacon = {"beacon": beacon_url}
                authors_object.append(beacon)
                authors_uuid_list.append(authors_dict[author])

            if len(authors_object) > 0:
                paper_object['hasAuthors'] = authors_object
                paper_authors_uuids_dict[paper_uuid] = authors_uuid_list

        batch.add_thing(paper_object, "Paper", paper_uuid)
        no_items_in_batch += 1

        if no_items_in_batch >= batch_size:
            no_imported = send_batch(client, 'Paper', batch, no_imported)
            # no_imported += no_items_in_batch
            batch = weaviate.ThingsBatchRequest()
            no_items_in_batch = 0

    if no_items_in_batch > 0:
        no_imported = send_batch(client, 'Paper', batch, no_imported)

    log('Done adding Papers')
    time.sleep(2)
    return paper_authors_uuids_dict


def add_wrotepapers_cref(
        client: weaviate.client.Client,
        paper_authors_uuids_dict: dict,
        batch_size: int = 512):
    """[summary]

    :param client: python client connection
    :type client: weaviate.client.Client
    :param paper_authors_uuids_dict: uuids of authors per paper to add
    :type paper_authors_uuids_dict: dict
    :param batch_size: number of items in a batch, defaults to 512
    :type batch_size: int, optional
    """
    log('Start adding crefs from Authors:wrotePapers to Papers')

    batch = weaviate.ReferenceBatchRequest()

    no_items_in_batch = 0
    no_imported = 0

    for paper, authors in paper_authors_uuids_dict.items():
        for author in authors:
            batch.add_reference(author, "Author", "wrotePapers", paper)
            no_items_in_batch += 1

        if no_items_in_batch >= batch_size:
            results = client.batch.add_references(batch)
            for result in results:
                if result['result']['status'] != 'SUCCESS':
                    log(result['result'])
                else:
                    no_imported += 1
            log('{} (out of batch of {}) new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(
                no_imported, no_items_in_batch, no_imported))

            batch = weaviate.ReferenceBatchRequest()
            no_items_in_batch = 0

    no_imported_lastbatch = 0
    results = client.batch.add_references(batch)
    if results is not None:
        for result in results:
            if result['result']['status'] != 'SUCCESS':
                log(result['result'])
            else:
                no_imported_lastbatch += 1
                no_imported += 1
        log('{} (out of batch of {}) new wrotePapers references imported in last batch, {} total wrotePapers references imported.'.format(
            no_imported_lastbatch, no_items_in_batch, no_imported))

    log('Done adding crefs from Authors:wrotePapers to Papers')
    time.sleep(2)
    return


if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")

    data = get_metadata('./data/arxiv-metadata-oai.json')

    # get categories
    result = client.query.get.things("Category", ["name", "uuid"]).do()
    categories_list = result["data"]["Get"]["Things"]["Category"]
    categories_with_uuid = {}
    for category in categories_list:
        categories_with_uuid[category["name"]] = category["uuid"]

    journals = add_and_return_journals(client, data)
    authors = add_and_return_authors(client, data)
    papers = add_and_return_papers(client, data, categories_dict=categories_with_uuid, journals_dict=journals, authors_dict=authors)
    add_wrotepapers_cref(client, papers)
