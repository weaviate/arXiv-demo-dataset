import sys
import os
import weaviate
import argparse
import json
from project import create_schema, import_data, import_taxanomy, helper

default_args = {
    "metadata_file": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'data/arxiv-metadata-oai.json'),
    "schema": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'schema/schema.json'),
    "weaviate": "http://localhost:8080",
    "overwrite_schema": False,
    "n_papers": float('inf'),
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
        # Taking command line arguments from users
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-i',
            '--metadata_file',
            help='location and name of the arXiv metadata json file',
            type=str,
            default=os.path.join(
                os.path.dirname(
                    os.path.realpath(__file__)),
                'data/arxiv-metadata-oai.json'))
        parser.add_argument(
            '-s',
            '--schema',
            help='location and name of the schema',
            type=str,
            default=os.path.join(
                os.path.dirname(
                    os.path.realpath(__file__)),
                'schema/schema.json'))
        parser.add_argument(
            '-w',
            '--weaviate',
            help='weaviate url',
            type=str,
            default='http://localhost:8080')
        parser.add_argument(
            '-mp',
            '--n_papers',
            help='maximum number of papers to import',
            type=float,
            default=float('inf'))
        parser.add_argument(
            '-snp',
            '--skip_n_papers',
            help='number of papers to skip before starting the import',
            type=int,
            default=0)
        parser.add_argument(
            '-po',
            '--papers_only',
            help='skips all other data object imports except for papers if set to True',
            type=bool,
            default=False)
        parser.add_argument(
            '-sj',
            '--skip_journals',
            help='whether you want to skip the import of all the journals',
            type=bool,
            default=False)
        parser.add_argument(
            '-sa',
            '--skip_authors',
            help='whether you want to skip the import of all the authors',
            type=bool,
            default=False)
        parser.add_argument(
            '-st',
            '--skip_taxonomy',
            help='whether you want to skip the import of all the arxiv taxonomy objects',
            type=bool,
            default=False)
        parser.add_argument(
            '-to',
            '--timeout',
            help='max time out in seconds for the python client batching operations',
            type=int,
            default=20)
        parser.add_argument(
            '-ows',
            '--overwrite_schema',
            help='overwrites the schema if one is present and one is given',
            type=int,
            default=False)
        parser.add_argument(
            '-bs',
            '--batch_size',
            help='maximum number of data objects to be sent in one batch',
            type=int,
            default=512)

        args = parser.parse_args()
        arguments = vars(args)

    return arguments


if __name__ == "__main__":
    arguments = user_input()

    helper.log('Starting project build')

    timeout = (2, arguments["timeout"])
    config = weaviate.ClientConfig(timeout_config=timeout)
    client = weaviate.Client(arguments["weaviate"], client_config=config)
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

    # add journals, authors, papers and references
    data = import_data.get_metadata(
        datafile=arguments["metadata_file"],
        max_size=arguments["n_papers"])

    if not arguments["papers_only"] and not arguments["skip_journals"]:
        journals = import_data.add_and_return_journals(
            client=client,
            data=data,
            batch_size=arguments["batch_size"],
            n_papers=arguments["n_papers"],
            skip_n_papers=arguments["skip_n_papers"])

    if not arguments["papers_only"] and not arguments["skip_authors"]:
        authors = import_data.add_and_return_authors(
            client=client,
            data=data,
            batch_size=arguments["batch_size"],
            n_papers=arguments["n_papers"],
            skip_n_papers=arguments["skip_n_papers"])

    paper_authors_uuids_dict = import_data.add_and_return_papers(
        client=client,
        data=data,
        categories_dict=categories_with_uuid,
        journals_dict=journals,
        authors_dict=authors,
        batch_size=arguments["batch_size"],
        n_papers=arguments["n_papers"],
        skip_n_papers=arguments["skip_n_papers"])
    import_data.add_wrotepapers_cref(
        client=client,
        paper_authors_uuids_dict=paper_authors_uuids_dict)
