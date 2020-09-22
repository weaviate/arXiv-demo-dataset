#!/usr/bin/env python
# coding: utf-8

# # ArXiv dataset with Weaviate

# In[41]:


import weaviate
import json
import tqdm
import re
import time

year_pattern = r'([1-2][0-9]{3})'


# In[42]:


def get_client():
    client = weaviate.Client("http://localhost:8080")
    meta_info = client.get_meta()
    print(meta_info)
    return client


# In[43]:


client = get_client()


# In[44]:


# get ids of categories
def get_ids_of_categories():
    categories_with_uuids = client.query.get.things("Category", ["id", "uuid"]).with_limit(2000).do()
    categories_with_uuids = categories_with_uuids['data']['Get']['Things']['Category']
    categories_with_uuids_dict = {}
    for category in categories_with_uuids:
        categories_with_uuids_dict[category['id']] = category['uuid']
    return categories_with_uuids_dict


# In[45]:


categories_with_uuids_dict = get_ids_of_categories()


# In[46]:


# {title, doi, year, journalReference, arXivId, submitter, abstract, comments, hasCategories, versionHistory, lastestVersionCreated, lastestVersion, pdfLink, link, licence, reportNumber, hasAuthors, inJournal}
def get_metadata():
    with open('./data/arxiv-metadata-oai.json', 'r') as f:
        for line in f:
            yield line


# In[47]:


# test
def test_metadata():
    metadata = get_metadata()
    for paper in metadata:
        for k, v in json.loads(paper).items():
            print(f'{k}: {v}')
        break


# In[48]:


def get_journal_name(a_string):
    splitted = re.split('([0-9]+)', a_string)
    return splitted[0]


# In[49]:


def get_journal_uuid(name):
    # check if journal exists
    where_filter = {
      "path": ["name"],
      "operator": "Equal",
      "valueString": name
    }

    result = client.query.get.things("Journal", ["uuid"]).with_where(where_filter).with_limit(10000).do()
    
    journals = result['data']['Get']['Things']['Journal']
    if len(journals) > 0:
        return journals[0]["uuid"]
    else: # journal does not exist yet
        data_obj = {"name": name}
        create_result = client.data_object.create(data_obj, "Journal")
        time.sleep(1)
        return create_result


# In[50]:


def format_author_name(author):
    regex = re.compile(r'[\n\r\t\'\\\"\`]')
    return regex.sub('', author)


# In[51]:


def get_author_uuid(name):
    # check if journal exists
    where_filter = {
      "path": ["name"],
      "operator": "Equal",
      "valueString": name
    }

    result = client.query.get.things("Author", ["uuid"]).with_where(where_filter).with_limit(10000).do()
    authors = result['data']['Get']['Things']['Author']
    if len(authors) > 0:
        return authors[0]["uuid"]
    else: # journal does not exist yet
        data_obj = {"name": name}
        create_result = client.data_object.create(data_obj, "Author")
        time.sleep(1)
        return create_result


# In[52]:


def extract_year(paper_id):
    year = 2000 + int(paper_id[:2])
        
    return year


# In[53]:


def add_papers(no_papers_to_import=100):
    metadata = get_metadata()

    batch = weaviate.ThingsBatchRequest()
    no_papers_in_batch  = 0
    no_papers_imported = 0

    for paper in metadata:
        paper = json.loads(paper)
        paper_object = {}

        if paper["title"] is not None: paper_object["title"] = paper["title"].replace('\n', ' ')
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

        paper_object["hasCategories"] = []
        for category in paper["categories"][0].split(' '): # id of category
            # create beacon
            beacon_url = "weaviate://localhost/things/" + categories_with_uuids_dict[category]
            beacon = {"beacon": beacon_url}
            paper_object["hasCategories"].append(beacon)

        # journal
        if paper["journal-ref"] is not None:
            journal_name = get_journal_name(paper["journal-ref"])
            journal_uuid = get_journal_uuid(journal_name.replace('\n', ' '))

            beacon = "weaviate://localhost/things/" + journal_uuid
            paper_object['inJournal'] = [{
                "beacon": beacon
            }]

        # authors
        if paper["authors"] is not None:

            # remove everything between parentheses (twice for recursion)
            result = format_author_name(paper["authors"])
            result = re.sub(r'\(.*\)', '', result)
            result = re.sub("[\(\[\{].*?[\)\]\}]", "", result)

            authors = result.split(', ')

            authors_object = []
            for author in authors:
                author_uuid = get_author_uuid(author)
                beacon = "weaviate://localhost/things/" + author_uuid
                authors_object.append({'beacon': beacon})

            if len(authors_object) > 0:
                paper_object['hasAuthors'] = authors_object

        batch.add_thing(paper_object, "Paper") 
        no_papers_in_batch += 1
        no_papers_imported += 1
        if no_papers_in_batch > 100:
            result = client.batch.create_things(batch)
            batch = weaviate.ThingsBatchRequest()
            no_papers_in_batch = 0

        if no_papers_imported >= no_papers_to_import :
            result = client.batch.create_things(batch)
            print(result)
            return

    # TO DO: lastestVersionCreated, pdfLink, link, licence, hasAuthors}


# In[54]:


def add_articles_to_authors():
    query = "{Get {Things {Paper {uuid HasAuthors {... on Author {name uuid}}}}}}"
    result = client.query.raw(query)
    
    data = result['data']['Get']['Things']['Paper']
    
    for paper in data:
        paper_uuid = paper["uuid"]
        authors = paper["HasAuthors"]
        
        for author in authors:
            author_uuid = author["uuid"]
            client.data_object.reference.add(author_uuid, "wrotePapers", paper_uuid)


# In[55]:

def add_data(no_papers_to_import=100):
    add_papers(no_papers_to_import=no_papers_to_import)
    time.sleep(2)
    add_articles_to_authors()


# In[ ]:

if __name__ == "__main__":
    test_metadata()
    add_papers()
    time.sleep(2)
    add_articles_to_authors()


# %%
