import weaviate
import json
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
import copy
import uuid
try:
    from project.helper import log, generate_uuid, send_batch
except ModuleNotFoundError:
    from helper import log, generate_uuid, send_batch


def load_taxanomy() -> dict:
    """ load ArXiv taxonomy from https://arxiv.org/category_taxonomy

    :return: groups, archives and categories
    :rtype: dict
    """

    website_url = requests.get('https://arxiv.org/category_taxonomy').text
    soup = BeautifulSoup(website_url, 'lxml')

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

    for t in tags:
        if t.name == "h2":
            level_1_name = t.text
            level_2_code = t.text
            level_2_name = t.text
        elif t.name == "h3":
            raw = t.text
            level_2_code = re.sub(r"(.*)\((.*)\)", r"\2", raw)
            level_2_name = re.sub(r"(.*)\((.*)\)", r"\1", raw)
        elif t.name == "h4":
            raw = t.text
            level_3_code = re.sub(r"(.*) \((.*)\)", r"\1", raw)
            level_3_name = re.sub(r"(.*) \((.*)\)", r"\2", raw)
        elif t.name == "p":
            notes = t.text
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


def add_categories(client: weaviate.client.Client, categories: list, archives_with_uuids_dict: dict) -> dict:
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
    # add categories to weaviate
    log('Start adding Categories')
    batch = weaviate.ObjectsBatchRequest()

    category_ids = []

    categories_with_uuid = {}

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

    imported_items = send_batch(client, "Category", batch)

    log('Done adding Categories')
    time.sleep(2)
    return categories_with_uuid


def add_archives(client: weaviate.client.Client, archives: list, groups_with_uuids_dict: dict) -> dict:
    """ Add the ArXiv taxonomy archives to Weaviate

    :param client: python client connection
    :type client: weaviate.client.Client
    :param archives: archives from the taxanomy
    :type archives: list
    :param groups_with_uuids_dict: groups with uuids where the categories should link to
    :type groups_with_uuids_dict: dict
    :return: archives with uuids
    :rtype: dict
    """
    # add archives to weaviate
    log('Start adding Archives')

    batch = weaviate.ObjectsBatchRequest()

    archives_with_uuid = {}
    for archive in archives:
        uuid = generate_uuid('Archive', archive["name"])
        group_beacon = "weaviate://localhost/" + \
            groups_with_uuids_dict['group' + archive['inGroup']]
        archive['inGroup'] = [{
            "beacon": group_beacon
        }]
        batch.add(archive, "Archive", uuid)
        archives_with_uuid['archive' + archive["name"]] = uuid

    imported_items = send_batch(client, 'Archive', batch)

    log('Done adding Archives')
    time.sleep(2)
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
    # add groups to weaviate
    log('Start adding Groups')

    batch = weaviate.ObjectsBatchRequest()
    groups_with_uuid = {}
    for group in groups:
        uuid = generate_uuid('Group', group['name'])
        batch.add(group, "Group", uuid)
        groups_with_uuid['group' + group['name']] = uuid

    imported_items = send_batch(client, 'Groups', batch)

    log('Done adding Groups')
    time.sleep(2)
    return groups_with_uuid


def import_taxanomy(client: weaviate.client.Client) -> dict:
    """ gets the taxanomy and imports groups, archives and categories

    :param client: weaviate python client connection
    :type client: weaviate.client.Client
    :return: categories with uuids
    :rtype: dict
    """
    taxanomy_dict = load_taxanomy()

    groups_with_uuid = add_groups(client, taxanomy_dict["groups"])
    archives_with_uuid = add_archives(
        client, taxanomy_dict["archives"], groups_with_uuid)
    categories_with_uuid = add_categories(
        client, taxanomy_dict["categories"], archives_with_uuid)
    return categories_with_uuid


if __name__ == "__main__":
    client = weaviate.Client("http://localhost:8080")

    taxanomy_dict = load_taxanomy()

    groups_with_uuid = add_groups(client, taxanomy_dict["groups"])
    archives_with_uuid = add_archives(
        client, taxanomy_dict["archives"], groups_with_uuid)
    categories_with_uuid = add_categories(
        client, taxanomy_dict["categories"], archives_with_uuid)
