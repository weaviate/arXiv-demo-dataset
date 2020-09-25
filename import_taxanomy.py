#!/usr/bin/env python
# coding: utf-8

import weaviate
import json
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
import copy
import uuid

def get_client():
    client = weaviate.Client("http://localhost:8080")
    meta_info = client.get_meta()
    print(meta_info)
    return client

client = get_client()

def generate_uuid(classname, identifier):
    """ Generate an uuid
    :param namespace: allows to make identifiers unique if they come from different source systems.
                        E.g. google maps, osm, ...
    :param identifier: that is used to generate the uuid
    :return: properly formed uuid in form of string
    """
    name = classname + identifier
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))

def load_taxanomy():
    ## load taxonomy from https://arxiv.org/category_taxonomy
    website_url = requests.get('https://arxiv.org/category_taxonomy').text
    soup = BeautifulSoup(website_url,'lxml')

    root = soup.find('div',{'id':'category_taxonomy_list'})

    tags = root.find_all(["h2","h3","h4","p"], recursive=True)

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
            level_2_code = re.sub(r"(.*)\((.*)\)",r"\2",raw)
            level_2_name = re.sub(r"(.*)\((.*)\)",r"\1",raw)
        elif t.name == "h4":
            raw = t.text
            level_3_code = re.sub(r"(.*) \((.*)\)",r"\1",raw)
            level_3_name = re.sub(r"(.*) \((.*)\)",r"\2",raw)
        elif t.name == "p":
            notes = t.text
            level_1_names.append(level_1_name)
            level_2_names.append(level_2_name)
            level_2_codes.append(level_2_code)
            level_3_names.append(level_3_name)
            level_3_codes.append(level_3_code)
            level_3_notes.append(notes)

    df_taxonomy = pd.DataFrame({
        'group_name' : level_1_names,
        'archive_name' : level_2_names,
        'archive_id' : level_2_codes,
        'category_name' : level_3_names,
        'category_id' : level_3_codes,
        'category_description': level_3_notes

    })
    #df_taxonomy.to_csv("arxiv-metadata-ext-taxonomy.csv", index=False)
    #df_taxonomy.groupby(["group_name","archive_name"]).head(3)

    groups = [] # {name}
    archives = [] # {name, id, inGroup}
    categories = [] # {name, id, description, inArchive}

    group_names = list(set(level_1_names))
    for name in group_names:
        groups.append({"name": name})

    df_archives = pd.DataFrame({
        'inGroup' : level_1_names,
        'name' : level_2_names,
        'id' : level_2_codes

    })
    df_archives.drop_duplicates(inplace=True, ignore_index=True)
    archives = df_archives.to_dict(orient="records")

    df_categories = pd.DataFrame({
        'inArchive' : level_2_names,
        'name' : level_3_names,
        'id' : level_3_codes,
        'description' : level_3_notes
    })
    df_categories.drop_duplicates(inplace=True, ignore_index=True)
    categories = df_categories.to_dict(orient="records")
    
    return groups, archives, categories

def add_groups(groups):
    # add groups to weaviate
    batch = weaviate.ThingsBatchRequest()
    groups_with_uuid = {}
    for group in groups:
        uuid = generate_uuid('Group', group['name'])
        batch.add_thing(group, "Group", uuid)
        groups_with_uuid['group' + group['name']] = uuid
    results = client.batch.create_things(batch)
    for result in results: 
        if result['result']:
            print(result['result'])
    print('{} new Group objects imported in last batch.'.format(len(batch)))
    time.sleep(2)
    return groups_with_uuid

def get_ids_of_groups():
    # get ids of groups
    groups_with_uuids = client.query.get.things("Group", ["name", "uuid"]).do()
    groups_with_uuids = groups_with_uuids['data']['Get']['Things']['Group']
    groups_with_uuids_dict = {}
    for group in groups_with_uuids:
        groups_with_uuids_dict[group['name']] = group['uuid']
    return groups_with_uuids_dict

def add_archives(archives, groups_with_uuids_dict):
    # groups_with_uuids_dict = get_ids_of_groups()
    # add archives to weaviate
    batch = weaviate.ThingsBatchRequest()

    archives_copy = archives
    archives_with_uuid = {}
    for archive in archives_copy:
        uuid = generate_uuid('Archive', archive["name"])
        group_beacon = "weaviate://localhost/things/" + groups_with_uuids_dict['group' + archive['inGroup']]
        archive['inGroup'] = [{
            "beacon": group_beacon
        }]
        batch.add_thing(archive, "Archive", uuid)
        archives_with_uuid['archive' + archive["name"]] = uuid

    results = client.batch.create_things(batch)
    for result in results: 
        if result['result']:
            print(result['result'])
    print('{} new Archive objects imported in last batch.'.format(len(batch)))
    time.sleep(2)
    return archives_with_uuid

def get_ids_of_archives():
    # get ids of archives
    archives_with_uuids = client.query.get.things("Archive", ["name", "uuid"]).do()
    archives_with_uuids = archives_with_uuids['data']['Get']['Things']['Archive']
    archives_with_uuids_dict = {}
    for archive in archives_with_uuids:
        archives_with_uuids_dict[archive['name']] = archive['uuid']
    return archives_with_uuids_dict

def add_categories(categories, archives_with_uuids_dict):
    
    #archives_with_uuids_dict = get_ids_of_archives()
    
    # add categories to weaviate
    batch = weaviate.ThingsBatchRequest()

    category_ids = []

    categories_with_uuid = {}

    for category in categories:
        category_copy = copy.deepcopy(category)
        category_uuid = category_copy["name"]
        uuid = generate_uuid('Category', category_uuid)
        archive_beacon = "weaviate://localhost/things/" + archives_with_uuids_dict['archive' + category['inArchive']]
        category_copy['inArchive'] = [{
            "beacon": archive_beacon
        }]
        batch.add_thing(category_copy, "Category", uuid)
        categories_with_uuid[category_copy["id"]] = uuid

        # also create archive for the category archive if not exist yet (e.g. "cs" for the category id "cs.AI"), because some items are labeled wrong in the dataset

        # check if archive exists
        if (category['id'].split('.')[0] not in category_ids) and (category['id'].split('.')[0] != category['id']):
            category_ids.append(category['id'].split('.')[0])

            extra_category = {}
            extra_category["name"] = category['inArchive']
            extra_category["id"] = category['id'].split('.')[0]
            extra_category['inArchive'] = [{
                "beacon": archive_beacon
            }]
            uuid = generate_uuid('ExtraCategory', extra_category["name"])
            batch.add_thing(extra_category, "Category", uuid)
            categories_with_uuid[extra_category["id"]] = uuid

    results = client.batch.create_things(batch)
    for result in results: 
        if result['result']:
            print(result['result'])
    print('{} new Category objects imported in last batch.'.format(len(batch)))
    time.sleep(2)

def add_full_taxanomy():
    groups, archives, categories = load_taxanomy()
    groups_with_uuid = add_groups(groups)
    archives_with_uuid = add_archives(archives, groups_with_uuid)
    categories_with_uuid = add_categories(categories, archives_with_uuid)
    return categories_with_uuid

if __name__ == "__main__":
    groups, archives, categories = load_taxanomy()
    groups_with_uuid = add_groups(groups)
    archives_with_uuid = add_archives(archives, groups_with_uuid)
    categories_with_uuid = add_categories(categories, archives_with_uuid)