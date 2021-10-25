#!/usr/bin/env python3
""" Load the data into Weaviate """

import time
import yaml

from modules.taxanomy import load_taxanomy
from modules.taxanomy import add_groups
from modules.taxanomy import add_archives
from modules.taxanomy import add_categories
from modules.metadata import get_metadata
from modules.imports import import_journals
from modules.imports import import_authors
from modules.imports import import_papers
from modules.imports import cross_reference
from modules.utilities import load_schema
from modules.utilities import get_weaviate_client


def _load_arxiv_demo():

    with open('./config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    if config is not None and 'weaviate' in config and 'data' in config:
        client = get_weaviate_client(config['weaviate'])
        load_schema(client, config)

        taxanomy = load_taxanomy(config)
        groups = add_groups(client, taxanomy["groups"])
        archives = add_archives(client, taxanomy["archives"], groups)
        categories = add_categories(client, taxanomy["categories"], archives)

        data = get_metadata(config)
        journals = import_journals(client, config, data)
        authors = import_authors(client, config, data)
        papers = import_papers(client, config, data, categories, journals, authors)
        cross_reference(client, config, papers)


###############################################################################################
# only the call for the main function below this line
###############################################################################################


def main():
    """ main """
    start = time.time()

    _load_arxiv_demo()

    end = time.time()
    minutes = round((end-start)/60)
    print("Total time required:", minutes, "minutes", round((end-start)%60, 1), "seconds")


main()
