""" Load and import taxanomy """

import re
import copy
import weaviate
from bs4 import BeautifulSoup
import requests
import pandas as pd

from modules.utilities import generate_uuid
from modules.utilities import check_batch_result


def load_taxanomy(config) -> dict:
    """ load ArXiv taxonomy from https://arxiv.org/category_taxonomy

    :return: groups, archives and categories
    :rtype: dict
    """
    #pylint: disable="too-many-locals"
    #pylint: disable="too-many-statements"

    path = 'taxanomy.html'
    if config is not None and 'data' in config and 'taxanomy' in config['data']:
        path = config['data']['taxanomy']

    try:
        response = requests.get('https://arxiv.org/category_taxonomy').text
        with open(path, "w") as file:
            file.write(response)
            file.close()
    except requests.exceptions.RequestException:
        with open(path, "r") as file:
            response = file.read()

    soup = BeautifulSoup(response, 'lxml')
    root = soup.find('div', {'id': 'category_taxonomy_list'})
    tags = root.find_all(["h2", "h3", "h4", "p"], recursive=True)

    level_1_name = ""
    level_2_code = ""
    level_2_name = ""

    level_1_names = []
    level_2_codes = []
    level_2_names = []
    level_3_codes = []
    level_3_names = []
    level_3_notes = []

    for tag in tags:
        if tag.name == "h2":
            level_1_name = tag.text
            level_2_code = tag.text
            level_2_name = tag.text
        elif tag.name == "h3":
            raw = tag.text
            level_2_code = re.sub(r"(.*)\((.*)\)", r"\2", raw)
            level_2_name = re.sub(r"(.*)\((.*)\)", r"\1", raw)
        elif tag.name == "h4":
            raw = tag.text
            level_3_code = re.sub(r"(.*) \((.*)\)", r"\1", raw)
            level_3_name = re.sub(r"(.*) \((.*)\)", r"\2", raw)
        elif tag.name == "p":
            notes = tag.text
            level_1_names.append(level_1_name)
            level_2_names.append(level_2_name)
            level_2_codes.append(level_2_code)
            level_3_names.append(level_3_name)
            level_3_codes.append(level_3_code)
            level_3_notes.append(notes)

    groups = []  # {name}
    archives = []  # {name, id, inGroup}
    categories = []  # {name, id, description, inArchive}

    group_names = list(set(level_1_names))
    for name in group_names:
        groups.append({"name": name})

    df_archives = pd.DataFrame({
        'inGroup': level_1_names,
        'name': level_2_names,
        'id': level_2_codes

    })
    df_archives.drop_duplicates(inplace=True, ignore_index=True)
    archives = df_archives.to_dict(orient="records")

    df_categories = pd.DataFrame({
        'inArchive': level_2_names,
        'name': level_3_names,
        'id': level_3_codes,
        'description': level_3_notes
    })
    df_categories.drop_duplicates(inplace=True, ignore_index=True)
    categories = df_categories.to_dict(orient="records")

    return {"groups": groups, "archives": archives, "categories": categories}


def add_categories(client, categories, archives_with_uuids_dict) -> dict:
    """ Add the ArXiv categories groups to Weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param categories: categories from the taxanomy
    :type categories: list
    :param archives_with_uuids_dict: archives with uuids where the categories should link to
    :type archives_with_uuids_dict: dict
    :return: categories with uuids
    :rtype: dict
    """

    batch = weaviate.ObjectsBatchRequest()
    category_ids = []
    categories_with_uuid = {}
    count = 0
    print("Start adding Categories ---------------:", count, end='\r')
    for category in categories:
        category_copy = copy.deepcopy(category)
        category_uuid = category_copy["name"]
        uuid = generate_uuid('Category', category_uuid)
        archive_beacon = "weaviate://localhost/" + \
            archives_with_uuids_dict['archive' + category['inArchive']]
        category_copy['inArchive'] = [{
            "beacon": archive_beacon
        }]
        batch.add(category_copy, "Category", uuid)
        categories_with_uuid[category_copy["id"]] = uuid

        # also create archive for the category archive if not exist yet (e.g.
        # "cs" for the category id "cs.AI"), because some items are labeled
        # wrong in the dataset

        # check if archive exists
        if (category['id'].split('.')[0] not in category_ids) and (
                category['id'].split('.')[0] != category['id']):
            category_ids.append(category['id'].split('.')[0])

            extra_category = {}
            extra_category["name"] = category['inArchive']
            extra_category["id"] = category['id'].split('.')[0]
            extra_category['inArchive'] = [{
                "beacon": archive_beacon
            }]
            uuid = generate_uuid('ExtraCategory', extra_category["name"])
            batch.add(extra_category, "Category", uuid)
            categories_with_uuid[extra_category["id"]] = uuid
            count += 1

    result = client.batch.create_objects(batch)
    check_batch_result(result)

    print("Done adding Categories ----------------:", count)
    return categories_with_uuid


def add_archives(client, archives, groups) -> dict:
    """ Add the ArXiv taxonomy archives to Weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param archives: archives from the taxanomy
    :type archives: list
    :param groups: groups with uuids where the categories should link to
    :type groups: dict
    :return: archives with uuids
    :rtype: dict
    """

    batch = weaviate.ObjectsBatchRequest()
    archives_with_uuid = {}
    count = 0
    print("Start adding Archives -----------------:", count, end='\r')
    for archive in archives:
        uuid = generate_uuid('Archive', archive["name"])
        group_beacon = "weaviate://localhost/" + groups['group' + archive['inGroup']]
        archive['inGroup'] = [{ "beacon": group_beacon }]
        batch.add(archive, "Archive", uuid)
        archives_with_uuid['archive' + archive["name"]] = uuid
        count += 1

    result = client.batch.create_objects(batch)
    check_batch_result(result)

    print("Done adding Archives ------------------:", count)
    return archives_with_uuid


def add_groups(client: weaviate.client.Client, groups: list) -> dict:
    """ Add the ArXiv taxonomy groups to Weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param groups: the groups in the taxanomy
    :type groups: list
    :return: groups with uuids
    :rtype: dict
    """

    batch = weaviate.ObjectsBatchRequest()
    groups_with_uuid = {}
    count = 0
    print("Start adding Groups -------------------:", count, end='\r')
    for group in groups:
        uuid = generate_uuid('Group', group['name'])
        batch.add(group, "Group", uuid)
        groups_with_uuid['group' + group['name']] = uuid
        count += 1

    result = client.batch.create_objects(batch)
    check_batch_result(result)
    print("Done adding Groups --------------------:", count)
    return groups_with_uuid
