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
    "n_papers": 10,
    "skip_n_papers": 0,
    "papers_only": False,
    "skip_journals": False,
    "skip_authors": False,
    "skip_taxonomy": False,
    "timeout": 20,
    "batch_size": 5}

class TestDataImport(unittest.TestCase):

    def setUp(self) -> None:
        create_schema.create_schema(schema='project/schema.json', overwrite=True)
        self.client = weaviate.Client("http://localhost:8080")

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

    def test_get_data(self):
        data = helper.get_metadata(
            datafile=arguments["metadata_file"],
            max_size=arguments["n_papers"])
        
        self.assertEqual(len(data), arguments["n_papers"])
        self.assertIn('id', data[0])