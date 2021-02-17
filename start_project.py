import sys
import os
import weaviate
import argparse
import json
from datetime import datetime
from project import create_schema, import_data, import_taxanomy, helper

default_args = {
    "metadata_file": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'data/arxiv-metadata-oai-snapshot.json'),
    "schema": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'project/schema.json'),
    "weaviate": "http://localhost:8080",
    "overwrite_schema": False,
    "n_papers": 1000000000,
    "skip_n_papers": 0,
    "papers_only": False,
    "skip_journals": False,
    "skip_authors": False,
    "skip_taxonomy": False,
    "timeout": 20,
    "batch_size": 512}


def user_input() -> dict:
    """ Parse the configuration arguments

    :return: arguments with values
    :rtype: dict
    """
    config = argparse.ArgumentParser()
    config.add_argument(
        '-cf',
        '--config_file',
        help='config file name',
        default='',
        type=str,
        required=False)
    config.add_argument(
        '-i',
        '--metadata_file',
        help='location and name of the arXiv metadata json file',
        type=str,
        default=os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)),
            'data/arxiv-metadata-oai-snapshot.json'))
    config.add_argument(
        '-s',
        '--schema',
        help='location and name of the schema',
        type=str,
        default=os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)),
            'project/schema.json'))
    config.add_argument(
        '-w',
        '--weaviate',
        help='weaviate url',
        type=str,
        default='http://localhost:8080')
    config.add_argument(
        '-np',
        '--n_papers',
        help='maximum number of papers to import',
        type=int,
        default=1000000000)
    config.add_argument(
        '-snp',
        '--skip_n_papers',
        help='number of papers to skip before starting the import',
        type=int,
        default=0)
    config.add_argument(
        '-po',
        '--papers_only',
        help='skips all other data object imports except for papers if set to True',
        type=bool,
        default=False)
    config.add_argument(
        '-sj',
        '--skip_journals',
        help='whether you want to skip the import of all the journals',
        type=bool,
        default=False)
    config.add_argument(
        '-sa',
        '--skip_authors',
        help='whether you want to skip the import of all the authors',
        type=bool,
        default=False)
    config.add_argument(
        '-st',
        '--skip_taxonomy',
        help='whether you want to skip the import of all the arxiv taxonomy objects',
        type=bool,
        default=False)
    config.add_argument(
        '-to',
        '--timeout',
        help='max time out in seconds for the python client batching operations',
        type=int,
        default=20)
    config.add_argument(
        '-ows',
        '--overwrite_schema',
        help='overwrites the schema if one is present and one is given',
        type=int,
        default=False)
    config.add_argument(
        '-bs',
        '--batch_size',
        help='maximum number of data objects to be sent in one batch',
        type=int,
        default=512)

    config_file_check = config.parse_known_args()
    object_check = vars(config_file_check[0])

    if object_check['config_file'] != '':
        json_file = json.load(open(config_file_check[0].config_file))
        arguments = default_args
        for key, value in json_file.items():
            if key in default_args:
                arguments[key] = value
            else:
                print(
                    'Variable "{}" with value "{}" not found in argument list, default values will be used instead'.format(
                        key,
                        value))
    else:
        args = config.parse_args()
        arguments = vars(args)

    return arguments


if __name__ == "__main__":
    arguments = user_input()

    startTime = datetime.now()
    helper.log('Starting project build')

    timeout = (2, arguments["timeout"])
    client = weaviate.Client(arguments["weaviate"], timeout_config=timeout)
    if not client.is_ready():
        raise Exception(
            'Weaviate on url {} is not ready, please check the url and try again'.format(
                arguments["weaviate"]))

    # create the schema
    create_schema.create_schema(
        arguments["schema"],
        arguments["weaviate"],
        arguments["overwrite_schema"])

    if not arguments["papers_only"] and not arguments["skip_taxonomy"]:
        # import the taxanomy
        taxanomy_dict = import_taxanomy.load_taxanomy()

        groups_with_uuid = import_taxanomy.add_groups(
            client=client, groups=taxanomy_dict["groups"])
        archives_with_uuid = import_taxanomy.add_archives(
            client=client,
            archives=taxanomy_dict["archives"],
            groups_with_uuids_dict=groups_with_uuid)
        categories_with_uuid = import_taxanomy.add_categories(
            client=client,
            categories=taxanomy_dict["categories"],
            archives_with_uuids_dict=archives_with_uuid)
    else:  # get categories from weaviate
        result = client.query.get.things("Category", ["name", "uuid"]).do()
        categories_list = result["data"]["Get"]["Category"]
        categories_with_uuid = {}
        for category in categories_list:
            categories_with_uuid[category["name"]] = category["uuid"]

    # add journals, authors, papers and references
    data = helper.get_metadata(
        datafile=arguments["metadata_file"],
        max_size=arguments["n_papers"],
        skip_n_papers=arguments["skip_n_papers"])

    if not arguments["papers_only"] and not arguments["skip_journals"]:
        journals = import_data.add_and_return_journals(
            client=client,
            data=data,
            batch_size=arguments["batch_size"],
            n_papers=arguments["n_papers"])
    else:  # get journals from weaviate
        result = client.query.get.things("Journal", ["name", "uuid"]).do()
        journals_list = result["data"]["Get"]["Journal"]
        journals = {}
        for journal in journals_list:
            journals[journal["name"]] = journal["uuid"]

    if not arguments["papers_only"] and not arguments["skip_authors"]:
        authors = import_data.add_and_return_authors(
            client=client,
            data=data,
            batch_size=arguments["batch_size"],
            n_papers=arguments["n_papers"])
    else:  # get journals from weaviate
        result = client.query.get.things("Author", ["name", "uuid"]).do()
        authors_list = result["data"]["Get"]["Author"]
        authors = {}
        for author in authors_list:
            authors[category["name"]] = author["uuid"]

    paper_authors_uuids_dict = import_data.add_and_return_papers(
        client=client,
        data=data,
        categories_dict=categories_with_uuid,
        journals_dict=journals,
        authors_dict=authors,
        batch_size=arguments["batch_size"],
        n_papers=arguments["n_papers"])
    import_data.add_wrotepapers_cref(
        client=client,
        paper_authors_uuids_dict=paper_authors_uuids_dict)

    helper.log('Done with the import, total time took {}'.format(str(datetime.now() - startTime)))
