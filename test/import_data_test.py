import unittest
import os
import weaviate
from project import create_schema, import_data, import_taxanomy, helper

arguments = {
    "metadata_file": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'sample-arxiv-metadata-oai.json'),
    "schema": os.path.join(
        os.path.dirname(
            os.path.realpath(__file__)),
        'project/schema.json'),
    "weaviate": "http://localhost:8080",
    "overwrite_schema": False,
    "n_papers": 1,
    "skip_n_papers": 0,
    "papers_only": False,
    "skip_journals": False,
    "skip_authors": False,
    "skip_taxonomy": False,
    "timeout": 20,
    "batch_size": 5}


class TestDataImport(unittest.TestCase):

    def setUp(self) -> None:
        weaviate_url = "http://localhost:8080"
        self.client = weaviate.Client(weaviate_url)
        create_schema.create_schema(schema='project/schema.json', weaviate_url=weaviate_url, overwrite=True)

    def test_load_taxanomy(self):
        taxanomy = import_taxanomy.load_taxanomy()

        self.assertIn('groups', taxanomy)
        self.assertIn('archives', taxanomy)
        self.assertIn('categories', taxanomy)
        self.assertGreater(len(taxanomy['groups']), 0)
        self.assertGreater(len(taxanomy['archives']), 0)
        self.assertGreater(len(taxanomy['categories']), 0)

    def test_import_taxanomy_classes(self):
        taxanomy = import_taxanomy.load_taxanomy()

        groups_with_uuid = import_taxanomy.add_groups(self.client, taxanomy["groups"])
        groups_added = self.client.query.aggregate.things('Group').with_fields('meta { count }').do()
        self.assertEqual(groups_added['data']['Aggregate']['Things']['Group'][0]['meta']['count'], len(taxanomy['groups']))

        archives_with_uuid = import_taxanomy.add_archives(
            self.client, taxanomy["archives"], groups_with_uuid)
        groups_added = self.client.query.aggregate.things('Archive').with_fields('meta { count }').do()
        self.assertEqual(groups_added['data']['Aggregate']['Things']['Archive'][0]['meta']['count'], len(taxanomy['archives']))

        categories_with_uuid = import_taxanomy.add_categories(
            self.client, taxanomy["categories"], archives_with_uuid)
        groups_added = self.client.query.aggregate.things('Category').with_fields('meta { count }').do()
        self.assertGreaterEqual(groups_added['data']['Aggregate']['Things']['Category'][0]['meta']['count'], len(taxanomy['categories']))

        # test cref
        query = """{
            Get {
                Things {
                    Category(where: {
                        path: ["name"],
                        operator: Equal,
                        valueString: "Artificial Intelligence"
                    }) {
                        name
                        InArchive {
                            ... on Archive {
                                name
                            }
                        }
                    }
                }
            }
        }"""

        query_result = self.client.query.raw(query)
        self.assertEqual(query_result['data']['Get']['Things']['Category'][0]['InArchive'][0]['name'], "Computer Science")

    def test_get_data(self):
        data = helper.get_metadata(
            datafile=arguments["metadata_file"],
            max_size=arguments["n_papers"])

        self.assertEqual(len(data), arguments["n_papers"])
        self.assertIn('id', data[0])

    def test_import_journal(self):
        data = helper.get_metadata(
            datafile=arguments["metadata_file"],
            max_size=arguments["n_papers"])
        journals = import_data.add_and_return_journals(self.client, data, n_papers=1)

        result = self.client.query.get.things("Journal", ["name"]).do()
        self.assertEqual(result['data']['Get']['Things']['Journal'][0]['name'], "Phys.Rev.D")

    def test_import_authors(self):
        data = helper.get_metadata(
            datafile=arguments["metadata_file"],
            max_size=arguments["n_papers"])
        authors = import_data.add_and_return_authors(self.client, data, n_papers=1)
        author_names = helper.format_author_name(data[0]["authors"])

        result = self.client.query.get.things("Author", ["name"]).do()
        imported_authors = []
        for author in result['data']['Get']['Things']['Author']:
            imported_authors.append(author["name"])

        self.assertEqual(sorted(imported_authors), sorted(author_names))

    def test_import_papers(self):
        data = helper.get_metadata(
            datafile=arguments["metadata_file"],
            max_size=arguments["n_papers"])
        categories = import_taxanomy.import_taxanomy(self.client)
        journals = import_data.add_and_return_journals(self.client, data, n_papers=1)
        authors = import_data.add_and_return_authors(self.client, data, n_papers=1)
        papers = import_data.add_and_return_papers(self.client, data, categories, journals, authors, n_papers=1)

        query = "{Get {Things {Paper {title HasCategories {... on Category {uuid}} HasAuthors {... on Author {name}}}}}}"
        result = self.client.query.raw(query)

        imported_title = result['data']['Get']['Things']['Paper'][0]['title']
        self.assertEqual(imported_title, data[0]['title'].replace('\n', ' '))

        author_names = helper.format_author_name(data[0]["authors"])
        imported_authors = []
        for author in result['data']['Get']['Things']['Paper'][0]['HasAuthors']:
            imported_authors.append(author["name"])
        self.assertEqual(sorted(imported_authors), sorted(author_names))

        category_uuid = categories[data[0]["categories"].split(' ')[0]]
        imported_category = result['data']['Get']['Things']['Paper'][0]['HasCategories'][0]['uuid']
        self.assertEqual(imported_category, category_uuid)
