""" Import routines """

import weaviate
from dateutil import parser
from modules.utilities import format_journal_name
from modules.utilities import format_author_name
from modules.utilities import generate_uuid
from modules.utilities import check_batch_result
from modules.utilities import extract_year
from modules.utilities import DEFAULT_MAX_BATCH


def import_journals(client, config, data) -> dict:
    """ Adds journals of the papers to weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param config: the config file with parameters
    :type data: dict
    :param data: the metadata of all papers to add
    :type data: list
    :return: journals with uuids
    :rtype: dict
    """
    # add groups to weaviate

    journals = {}
    batch = weaviate.ObjectsBatchRequest()
    batchcount = totalcount = 0
    maxbatch = DEFAULT_MAX_BATCH
    if config is not None and 'weaviate' in config and 'max_batch_size' in config['weaviate']:
        maxbatch = config['weaviate']['max_batch_size']

    for paper in data:
        if 'journal-ref' in paper and paper['journal-ref'] is not None:
            journal_name = format_journal_name(paper['journal-ref'])
            journal_uuid = generate_uuid('Journal', journal_name)

            if journal_name not in journals:
                batch.add({"name": journal_name}, "Journal", journal_uuid)
                batchcount += 1
                totalcount += 1
                journals[journal_name] = journal_uuid

            if batchcount >= maxbatch:
                result = client.batch.create_objects(batch)
                check_batch_result(result)
                batch = weaviate.ObjectsBatchRequest()
                print("Importing journals to Weaviate --------:", totalcount, end="\r")
                batchcount = 0

    if batchcount > 0:
        result = client.batch.create_objects(batch)
        check_batch_result(result)
    print("Done importing journals to Weaviate ---:", totalcount)

    return journals


def import_authors(client, config, data) -> dict:
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

    authors_uuid = {}
    batch = weaviate.ObjectsBatchRequest()
    batchcount = totalcount = 0
    maxbatch = DEFAULT_MAX_BATCH
    if config is not None and 'weaviate' in config and 'max_batch_size' in config['weaviate']:
        maxbatch = config['weaviate']['max_batch_size']

    for paper in data:
        if paper["authors"] is not None:
            authors = format_author_name(paper["authors"])

            for author in authors:
                if author not in authors_uuid:
                    author_uuid = generate_uuid('Author', author)
                    batch.add({"name": author}, "Author", author_uuid)
                    batchcount += 1
                    totalcount += 1
                    authors_uuid[author] = author_uuid

                if batchcount >= maxbatch:
                    result = client.batch.create_objects(batch)
                    check_batch_result(result)
                    batch = weaviate.ObjectsBatchRequest()
                    print("Importing authors to Weaviate ---------:", totalcount, end="\r")
                    batchcount = 0

    if batchcount > 0:
        result = client.batch.create_objects(batch)
        check_batch_result(result)
    print("Done importing authors to Weaviate ----:", totalcount)
    return authors_uuid


def import_papers(client, config, data, categories, journals, authors_uuid) -> dict:
    """[summary]

    :param client: python client connection
    :type client: weaviate.client.Client
    :param data: the metadata of all papers to add
    :type data: list
    :param categories: categories with uuids
    :type categories: dict
    :param journals: journals with uuids
    :type journals: dict
    :param authors: authors with uuids
    :type authors: dict
    :param batch_size: number of items in a batch, defaults to 512
    :type batch_size: int, optional
    :param n_papers: number of papers to import in total, defaults to 1000000000
    :type n_papers: float, optional
    :return: uuids of all papers with uuids the authors
    :rtype: dict
    """

    paper_authors_uuids_dict = {}
    batch = weaviate.ObjectsBatchRequest()
    batchcount = totalcount = 0
    maxbatch = DEFAULT_MAX_BATCH
    if config is not None and 'weaviate' in config and 'max_batch_size' in config['weaviate']:
        maxbatch = config['weaviate']['max_batch_size']

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

            # older arxiv datadump files use versions in a string in one list item
            if isinstance(paper["versions"][0], str):
                paper_object["versionHistory"] = str(paper["versions"]).strip('[]')
                paper_object["latestVersion"] = paper["versions"][-1]
                uuid_base += paper_object["latestVersion"]

            # lastest arxiv datadump file uses different version datatypes
            elif isinstance(paper["versions"][0], dict):
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

        paper_uuid = generate_uuid('Paper', uuid_base)

        # try to extract year
        if paper["id"] is not None:
            year = extract_year(paper["id"])
            paper_object["year"] = year

        if len(paper["categories"].split(' ')) >= 1:
            categories_object = []
            for category in paper["categories"].split(' '):  # id of category
                # create beacon
                if category not in categories:
                    break
                beacon_url = "weaviate://localhost/" + categories[category]
                beacon = {"beacon": beacon_url}
                categories_object.append(beacon)

            if len(categories_object) > 0:
                paper_object["hasCategories"] = categories_object

        if paper["journal-ref"] is not None:
            journal_name = format_journal_name(paper["journal-ref"])
            # create beacon
            if journal_name in journals:
                beacon_url = "weaviate://localhost/" + journals[journal_name]
                beacon = {"beacon": beacon_url}
                paper_object['inJournal'] = [beacon]

        if paper["authors"] is not None:
            authors = format_author_name(paper["authors"])
            authors_object = []
            authors_uuid_list = []

            for author in authors:
                if author not in authors_uuid:
                    break

                beacon_url = "weaviate://localhost/" + authors_uuid[author]
                beacon = {"beacon": beacon_url}
                authors_object.append(beacon)
                authors_uuid_list.append(authors_uuid[author])

            if len(authors_object) > 0:
                paper_object['hasAuthors'] = authors_object
                paper_authors_uuids_dict[paper_uuid] = authors_uuid_list

        batch.add(paper_object, "Paper", paper_uuid)
        batchcount += 1
        totalcount += 1

        if batchcount >= maxbatch:
            result = client.batch.create_objects(batch)
            check_batch_result(result)
            batch = weaviate.ObjectsBatchRequest()
            print("Importing papers to Weaviate ----------:", totalcount, end="\r")
            batchcount = 0

    if batchcount > 0:
        result = client.batch.create_objects(batch)
        check_batch_result(result)
    print("Done importating papers to Weaviate ---:", totalcount)

    return paper_authors_uuids_dict


def cross_reference(client, config, papers: dict):
    """[summary]

    :param client: python client connection
    :type client: weaviate.client.Client
    :param papers: uuids of authors per paper to add
    :type papers: dict
    """

    batch = weaviate.ReferenceBatchRequest()
    batchcount = totalcount = 0
    maxbatch = DEFAULT_MAX_BATCH
    if config is not None and 'weaviate' in config and 'max_batch_size' in config['weaviate']:
        maxbatch = config['weaviate']['max_batch_size']

    for paper, authors in papers.items():
        for author in authors:
            batch.add(author, "Author", "wrotePapers", paper)
            batchcount += 1
            totalcount += 1

        if batchcount >= maxbatch:
            result = client.batch.create_references(batch)
            check_batch_result(result)
            batch = weaviate.ReferenceBatchRequest()
            print("Cross referenced paper to author ------:", totalcount, end="\r")
            batchcount = 0

    if batchcount > 0:
        result = client.batch.create_references(batch)
        check_batch_result(result)
    print("Cross referenced paper to author ------:", totalcount)
